package bwbadger

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/dgraph-io/badger"
	"github.com/google/badwolf/storage"
	"github.com/google/badwolf/triple"
	"github.com/google/badwolf/triple/literal"
	"github.com/google/badwolf/triple/node"
	"github.com/google/badwolf/triple/predicate"
)

const (
	// Default BadgerDB discardRatio. It represents the discard ratio for the
	// BadgerDB GC.
	//
	// Ref: https://godoc.org/github.com/dgraph-io/badger#DB.RunValueLogGC
	badgerDiscardRatio = 0.5

	// Default BadgerDB GC interval
	badgerGCInterval = 5 * time.Minute
)

// driver implements BadWolf storage.Store for a fully compliant driver.
type driver struct {
	path string
	db   *badger.DB
	lb   literal.Builder

	cancelFunc context.CancelFunc
	ctx        context.Context

	rwmu sync.RWMutex
}

// graphBucket contains the name of the bucket containing the graphs.
const graphBucket = "GRAPHS"

// New create a new BadWolf driver using BoltDB as a storage driver.
func New(path string, lb literal.Builder, timeOut time.Duration, readOnly bool) (storage.Store, *badger.DB) {

	opts := badger.DefaultOptions(path).
		WithReadOnly(readOnly).
		WithSyncWrites(false).
		WithNumVersionsToKeep(1).
		WithLogger(nil)

	opts.Dir, opts.ValueDir = path, path
	// Open the DB.
	db, err := badger.Open(opts)
	if err != nil {
		log.Fatalf("bwbadger driver initialization failure for file %q %v", path, err)
	}

	// Return the initilized driver.
	driver := &driver{
		path: path,
		db:   db,
		lb:   lb,
	}

	driver.ctx, driver.cancelFunc = context.WithCancel(context.Background())
	go driver.runGC()

	return driver, db
}

// Name returns the ID of the backend being used.
func (d *driver) Name(ctx context.Context) string {
	return fmt.Sprintf("bwbadger/%s", d.path)
}

func (d *driver) runGC() {
	ticker := time.NewTicker(badgerGCInterval)
	for {
		select {
		case <-ticker.C:
			err := d.db.RunValueLogGC(badgerDiscardRatio)
			if err != nil {
				// don't report error when GC didn't result in any cleanup
				if err != badger.ErrNoRewrite {
					log.Printf("failed to GC BadgerDB: %v", err)
				}
			}

		case <-d.ctx.Done():
			return
		}
	}
}

// Version returns the version of the driver implementation.
func (d *driver) Version(ctx context.Context) string {
	return "HEAD"
}

// NewGraph creates a new graph. Creating an already existing graph
// should return an error.
func (d *driver) NewGraph(ctx context.Context, id string) (storage.Graph, error) {

	err := d.db.View(func(tx *badger.Txn) error {
		_, err := tx.Get([]byte(graphBucket + "/" + id))
		if err != badger.ErrKeyNotFound {
			return fmt.Errorf(": graph %q already exist", id)
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	tx := d.db.NewTransaction(true)
	defer tx.Discard()

	err = tx.Set([]byte(graphBucket+"/"+id), nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create new graph %q with error %v", id, err)
	}
	tx.Commit()

	return &graph{
		id: id,
		db: d.db,
		lb: d.lb,
	}, nil
}

// Graph returns an existing graph if available. Getting a non existing
// graph should return an error.
func (d *driver) Graph(ctx context.Context, id string) (storage.Graph, error) {
	err := d.db.View(func(tx *badger.Txn) error {
		_, err := tx.Get([]byte(graphBucket + "/" + id))
		if err == badger.ErrKeyNotFound {
			return fmt.Errorf(": graph %q does not exist", id)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &graph{
		id: id,
		db: d.db,
		lb: d.lb,
	}, nil
}

// DeleteGraph deletes an existing graph. Deleting a non existing graph
// should return an error.
func (d *driver) DeleteGraph(ctx context.Context, id string) error {
	d.rwmu.Lock()
	defer d.rwmu.Unlock()

	key := []byte(graphBucket + "/" + id)

	tx := d.db.NewTransaction(true)
	defer tx.Discard()

	_, err := tx.Get(key)
	if err == badger.ErrKeyNotFound {
		return fmt.Errorf(": graph %q does not exist", id)
	}
	keysC := make(chan string)

	go func() {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 1000
		opts.PrefetchValues = false
		opts.Prefix = key

		defer close(keysC)

		txI := d.db.NewTransaction(true)
		defer txI.Discard()

		it := txI.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			k := it.Item().Key()
			// println("send to deletion channel :" + string(k))
			keysC <- string(k)
		}
	}()

	txD := d.db.NewTransaction(true)
	defer txD.Discard()
	for k := range keysC {
		if err = txD.Delete([]byte(k)); err == badger.ErrTxnTooBig {
			_ = txD.Commit()
			txD = d.db.NewTransaction(true)
			_ = txD.Delete([]byte(k))
		} else if err != nil {
			return err
		}
	}
	err = txD.Commit()
	if err != nil {
		return err
	}
	return nil
}

// GraphNames returns the current available graph names in the store.
func (d *driver) GraphNames(ctx context.Context, names chan<- string) error {
	defer close(names)
	return d.db.View(func(tx *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := tx.NewIterator(opts)
		defer it.Close()
		for it.Seek([]byte(graphBucket)); it.ValidForPrefix([]byte(graphBucket)); it.Next() {
			item := it.Item()
			k := item.Key()
			names <- string(k[len(graphBucket)+1:]) // strips graphBucket+/
		}
		return nil
	})
}

// graph implements BadWolf storage.Graph for a fully compliant driver.
type graph struct {
	id string
	db *badger.DB
	lb literal.Builder
}

// ID returns the id for this graph.
func (g *graph) ID(ctx context.Context) string {
	return g.id
}

// indexUpdate contains the update to perform to a given index.
type indexUpdate struct {
	idx   string
	key   []byte
	value []byte
}

const (
	idxSPO = "SPO"
	idxSOP = "SOP"
	idxPOS = "POS"
	idxOPS = "OPS"
)

// Given a triple, returns the updates to perform to the indices.
func (g *graph) tripleToIndexUpdate(t *triple.Triple) []*indexUpdate {
	var updates []*indexUpdate
	s, p, o := t.Subject().String(), t.Predicate().String(), t.Object().String()
	tt := []byte(t.String())

	updates = append(updates,
		&indexUpdate{
			idx:   idxSPO,
			key:   []byte(s),
			value: nil,
		},
		&indexUpdate{
			idx:   idxSPO,
			key:   []byte(s + "\t" + p),
			value: nil,
		},
		&indexUpdate{
			idx:   idxSPO,
			key:   tt,
			value: tt,
		},
		&indexUpdate{
			idx:   idxSOP,
			key:   []byte(s),
			value: nil,
		},
		&indexUpdate{
			idx:   idxSOP,
			key:   []byte(s + "\t" + o),
			value: nil,
		},
		&indexUpdate{
			idx:   idxSOP,
			key:   []byte(s + "\t" + o + "\t" + p),
			value: tt,
		},
		&indexUpdate{
			idx:   idxPOS,
			key:   []byte(p),
			value: nil,
		},
		&indexUpdate{
			idx:   idxPOS,
			key:   []byte(p + "\t" + o),
			value: nil,
		},
		&indexUpdate{
			idx:   idxPOS,
			key:   []byte(p + "\t" + o + "\t" + s),
			value: tt,
		},
		&indexUpdate{
			idx:   idxOPS,
			key:   []byte(o),
			value: nil,
		},
		&indexUpdate{
			idx:   idxOPS,
			key:   []byte(o + "\t" + p),
			value: nil,
		},
		&indexUpdate{
			idx:   idxOPS,
			key:   []byte(o + "\t" + p + "\t" + s),
			value: tt,
		})

	return updates
}

// AddTriples adds the triples to the storage. Adding a triple that already
// exists should not fail. Efficiently resolving all the operations below
// require proper indexing. This driver provides the follow indices:
//
//   * SPO: Textual representation of the triple to allow range queries.
//   * SOP: Combination to allow efficient query of S + P queries.
//   * POS: Combination to allow efficient query of P + O queries.
//   * OPS: Combination to allow efficient query of O + P queries.
//
// The GUID index containst the fully serialized triple. The other indices
// only contains as a value the GUID of the triple.
func (g *graph) AddTriples(ctx context.Context, ts []*triple.Triple) error {
	tx := g.db.NewTransaction(true)
	defer tx.Discard()

	_, err := tx.Get([]byte(graphBucket + "/" + g.id))
	if err == badger.ErrKeyNotFound {
		return fmt.Errorf("graph %q does not exist", g.id)
	}

	for _, t := range ts {
		for _, iu := range g.tripleToIndexUpdate(t) {
			if err := tx.Set(append([]byte(graphBucket+"/"+g.id+"/"+iu.idx+"/"), iu.key...), iu.value); err == badger.ErrTxnTooBig {
				_ = tx.Commit()
				tx = g.db.NewTransaction(true)
				_ = tx.Set(append([]byte(graphBucket+"/"+g.id+"/"+iu.idx+"/"), iu.key...), iu.value)
			} else if err != nil {
				return err
			}
		}
	}

	return tx.Commit()
}

// RemoveTriples removes the trilpes from the storage. Removing triples that
// are not present on the store should not fail.
func (g *graph) RemoveTriples(ctx context.Context, ts []*triple.Triple) error {
	tx := g.db.NewTransaction(true)
	defer tx.Discard()

	_, err := tx.Get([]byte(graphBucket + "/" + g.id))
	if err == badger.ErrKeyNotFound {
		return fmt.Errorf("graph %q does not exist", g.id)
	}

	for _, t := range ts {
		for _, iu := range g.tripleToIndexUpdate(t) {
			if err := tx.Delete(append([]byte(graphBucket+"/"+g.id+"/"+iu.idx+"/"), iu.key...)); err == badger.ErrTxnTooBig {
				_ = tx.Commit()
				tx = g.db.NewTransaction(true)
				_ = tx.Delete(append([]byte(graphBucket+"/"+g.id+"/"+iu.idx+"/"), iu.key...))
			} else if err != nil {
				return err
			}
		}
	}

	return tx.Commit()
}

// shouldAccept returns is the triple should be accepted
func (g *graph) shouldAccept(cnt int, t *triple.Triple, lo *storage.LookupOptions) bool {
	if lo.MaxElements > 0 && lo.MaxElements < cnt {
		return false
	}
	p := t.Predicate()
	if p.Type() == predicate.Temporal {
		if lo.LowerAnchor != nil {
			ta, err := t.Predicate().TimeAnchor()
			if err != nil {
				panic(fmt.Errorf("should have never failed to retrieve time anchor from triple %s with error %v", t.String(), err))
			}
			if lo.LowerAnchor.After(*ta) {
				return false
			}
		}
		if lo.UpperAnchor != nil {
			ta, err := t.Predicate().TimeAnchor()
			if err != nil {
				panic(fmt.Errorf("should have never failed to retrieve time anchor from triple %s with error %v", t.String(), err))
			}
			if lo.UpperAnchor.Before(*ta) {
				return false
			}
		}
	}
	return true
}

// Objects pushes to the provided channel the objects for the given object and
// predicate. The function does not return immediately but spawns a goroutine
// to satisfy elements in the channel.
func (g *graph) Objects(ctx context.Context, s *node.Node, p *predicate.Predicate, lo *storage.LookupOptions, objs chan<- *triple.Object) error {
	defer close(objs)
	return g.db.View(func(tx *badger.Txn) error {

		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		opts.PrefetchSize = 1000
		opts.Prefix = []byte(graphBucket + "/" + g.id + "/" + idxSPO + "/" + s.String() + "\t" + p.String())

		it := tx.NewIterator(opts)
		defer it.Close()

		cnt := 0
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()

			err := item.Value(func(v []byte) error {

				if string(v) == "" {
					return nil
				}

				t, err := triple.Parse(string(v), g.lb)
				if err != nil {
					return fmt.Errorf("corrupt index SPO failing to parse %q with error %v", string(v), err)
				}

				if g.shouldAccept(cnt, t, lo) {
					objs <- t.Object()
					cnt++
				}

				return nil
			})

			if err != nil {
				return err
			}

		}

		return nil
	})
}

// Subjects pushes to the provided channel the subjects for the give predicate
// and object. The function does not return immediately but spawns a
// goroutine to satisfy elements in the channel.
func (g *graph) Subjects(ctx context.Context, p *predicate.Predicate, o *triple.Object, lo *storage.LookupOptions, subs chan<- *node.Node) error {
	defer close(subs)
	return g.db.View(func(tx *badger.Txn) error {

		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		opts.PrefetchSize = 1000
		opts.Prefix = []byte(graphBucket + "/" + g.id + "/" + idxPOS + "/" + p.String() + "\t" + o.String())

		it := tx.NewIterator(opts)
		defer it.Close()

		cnt := 0
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()

			err := item.Value(func(v []byte) error {

				if string(v) == "" {
					return nil
				}

				t, err := triple.Parse(string(v), g.lb)
				if err != nil {
					return fmt.Errorf("corrupt index POS failing to parse %q with error %v", string(v), err)
				}

				if g.shouldAccept(cnt, t, lo) {
					subs <- t.Subject()
					cnt++
				}

				return nil
			})

			if err != nil {
				return err
			}

		}

		return nil
	})
}

// PredicatesForSubject pushes to the provided channel all the predicates
// known for the given subject. The function does not return immediately but
// spawns a goroutine to satisfy elements in the channel.
func (g *graph) PredicatesForSubject(ctx context.Context, s *node.Node, lo *storage.LookupOptions, prds chan<- *predicate.Predicate) error {
	defer close(prds)
	return g.db.View(func(tx *badger.Txn) error {

		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		opts.PrefetchSize = 1000
		opts.Prefix = []byte(graphBucket + "/" + g.id + "/" + idxSPO + "/" + s.String())

		it := tx.NewIterator(opts)
		defer it.Close()

		cnt := 0
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()

			err := item.Value(func(v []byte) error {

				if string(v) == "" {
					return nil
				}

				t, err := triple.Parse(string(v), g.lb)
				if err != nil {
					return fmt.Errorf("corrupt index SPO failing to parse %q with error %v", string(v), err)
				}

				if g.shouldAccept(cnt, t, lo) {
					prds <- t.Predicate()
					cnt++
				}

				return nil
			})

			if err != nil {
				return err
			}

		}
		return nil
	})
}

// PredicatesForObject pushes to the provided channel all the predicates known
// for the given object. The function returns immediately and spawns a go
// routine to satisfy elements in the channel.
func (g *graph) PredicatesForObject(ctx context.Context, o *triple.Object, lo *storage.LookupOptions, prds chan<- *predicate.Predicate) error {
	defer close(prds)
	return g.db.View(func(tx *badger.Txn) error {

		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		opts.PrefetchSize = 1000
		opts.Prefix = []byte(graphBucket + "/" + g.id + "/" + idxOPS + "/" + o.String())

		it := tx.NewIterator(opts)
		defer it.Close()

		cnt := 0
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()

			err := item.Value(func(v []byte) error {

				if string(v) == "" {
					return nil
				}

				t, err := triple.Parse(string(v), g.lb)
				if err != nil {
					return fmt.Errorf("corrupt index OPS failing to parse %q with error %v", string(v), err)
				}

				if g.shouldAccept(cnt, t, lo) {
					prds <- t.Predicate()
					cnt++
				}

				return nil
			})

			if err != nil {
				return err
			}

		}
		return nil
	})
}

// PredicatesForSubjectAndObject pushes to the provided channel all predicates
// available for the given subject and object. The function does not return
// immediately but spawns a goroutine to satisfy elements in the channel.
func (g *graph) PredicatesForSubjectAndObject(ctx context.Context, s *node.Node, o *triple.Object, lo *storage.LookupOptions, prds chan<- *predicate.Predicate) error {
	defer close(prds)
	return g.db.View(func(tx *badger.Txn) error {

		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		opts.PrefetchSize = 1000
		opts.Prefix = []byte(graphBucket + "/" + g.id + "/" + idxSOP + "/" + s.String() + "\t" + o.String())

		it := tx.NewIterator(opts)
		defer it.Close()

		cnt := 0
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()

			err := item.Value(func(v []byte) error {

				if string(v) == "" {
					return nil
				}

				t, err := triple.Parse(string(v), g.lb)
				if err != nil {
					return fmt.Errorf("corrupt index SOP failing to parse %q with error %v", string(v), err)
				}

				if g.shouldAccept(cnt, t, lo) {
					prds <- t.Predicate()
					cnt++
				}

				return nil
			})

			if err != nil {
				return err
			}

		}
		return nil
	})
}

// TriplesForSubject pushes to the provided channel all triples available for
// the given subect. The function does not return immediately but spawns a
// goroutine to satisfy elements in the channel.
func (g *graph) TriplesForSubject(ctx context.Context, s *node.Node, lo *storage.LookupOptions, trpls chan<- *triple.Triple) error {
	defer close(trpls)
	return g.db.View(func(tx *badger.Txn) error {

		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		opts.PrefetchSize = 1000
		opts.Prefix = []byte(graphBucket + "/" + g.id + "/" + idxSPO + "/" + s.String())

		it := tx.NewIterator(opts)
		defer it.Close()

		cnt := 0
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()

			err := item.Value(func(v []byte) error {

				if string(v) == "" {
					return nil
				}

				t, err := triple.Parse(string(v), g.lb)
				if err != nil {
					return fmt.Errorf("corrupt index SPO failing to parse %q with error %v", string(v), err)
				}

				if g.shouldAccept(cnt, t, lo) {
					trpls <- t
					cnt++
				}

				return nil
			})

			if err != nil {
				return err
			}

		}
		return nil
	})

}

// TriplesForPredicate pushes to the provided channel all triples available
// for the given predicate.The function does not return immediately but spawns
// a goroutine to satisfy elements in the channel.
func (g *graph) TriplesForPredicate(ctx context.Context, p *predicate.Predicate, lo *storage.LookupOptions, trpls chan<- *triple.Triple) error {
	defer close(trpls)
	return g.db.View(func(tx *badger.Txn) error {

		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		opts.PrefetchSize = 1000
		opts.Prefix = []byte(graphBucket + "/" + g.id + "/" + idxPOS + "/" + p.String())

		it := tx.NewIterator(opts)
		defer it.Close()

		cnt := 0
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()

			err := item.Value(func(v []byte) error {

				if string(v) == "" {
					return nil
				}

				t, err := triple.Parse(string(v), g.lb)
				if err != nil {
					return fmt.Errorf("corrupt index POS failing to parse %q with error %v", string(v), err)
				}

				if g.shouldAccept(cnt, t, lo) {
					trpls <- t
					cnt++
				}

				return nil
			})

			if err != nil {
				return err
			}

		}
		return nil
	})
}

// TriplesForObject pushes to the provided channel all triples available for
// the given object. The function does not return immediately but spawns a
// goroutine to satisfy elements in the channel.
func (g *graph) TriplesForObject(ctx context.Context, o *triple.Object, lo *storage.LookupOptions, trpls chan<- *triple.Triple) error {
	defer close(trpls)
	return g.db.View(func(tx *badger.Txn) error {

		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		opts.PrefetchSize = 1000
		opts.Prefix = []byte(graphBucket + "/" + g.id + "/" + idxOPS + "/" + o.String())

		it := tx.NewIterator(opts)
		defer it.Close()

		cnt := 0
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()

			err := item.Value(func(v []byte) error {

				if string(v) == "" {
					return nil
				}

				t, err := triple.Parse(string(v), g.lb)
				if err != nil {
					return fmt.Errorf("corrupt index POS failing to parse %q with error %v", string(v), err)
				}

				if g.shouldAccept(cnt, t, lo) {
					trpls <- t
					cnt++
				}

				return nil
			})

			if err != nil {
				return err
			}

		}
		return nil
	})

}

// TriplesForSubjectAndPredicate pushes to the provided channel all triples
// available for the given subject and predicate. The function does not return
// immediately but spawns a goroutine to satisfy elements in the channel.
func (g *graph) TriplesForSubjectAndPredicate(ctx context.Context, s *node.Node, p *predicate.Predicate, lo *storage.LookupOptions, trpls chan<- *triple.Triple) error {
	defer close(trpls)
	return g.db.View(func(tx *badger.Txn) error {

		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		opts.PrefetchSize = 1000
		opts.Prefix = []byte(graphBucket + "/" + g.id + "/" + idxSPO + "/" + s.String() + "\t" + p.String())

		it := tx.NewIterator(opts)
		defer it.Close()

		cnt := 0
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()

			err := item.Value(func(v []byte) error {

				if string(v) == "" {
					return nil
				}

				t, err := triple.Parse(string(v), g.lb)
				if err != nil {
					return fmt.Errorf("corrupt index SPO failing to parse %q with error %v", string(v), err)
				}

				if g.shouldAccept(cnt, t, lo) {
					trpls <- t
					cnt++
				}

				return nil
			})

			if err != nil {
				return err
			}

		}
		return nil
	})
}

// TriplesForPredicateAndObject pushes to the provided channel all triples
// available for the given predicate and object. The function does not return
// immediately but spawns a goroutine to satisfy elements in the channel.
func (g *graph) TriplesForPredicateAndObject(ctx context.Context, p *predicate.Predicate, o *triple.Object, lo *storage.LookupOptions, trpls chan<- *triple.Triple) error {
	defer close(trpls)
	return g.db.View(func(tx *badger.Txn) error {

		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		opts.PrefetchSize = 1000
		opts.Prefix = []byte(graphBucket + "/" + g.id + "/" + idxPOS + "/" + p.String() + "\t" + o.String())

		it := tx.NewIterator(opts)
		defer it.Close()

		cnt := 0
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()

			err := item.Value(func(v []byte) error {

				if string(v) == "" {
					return nil
				}

				t, err := triple.Parse(string(v), g.lb)
				if err != nil {
					return fmt.Errorf("corrupt index SPO failing to parse %q with error %v", string(v), err)
				}

				if g.shouldAccept(cnt, t, lo) {
					trpls <- t
					cnt++
				}

				return nil
			})

			if err != nil {
				return err
			}

		}
		return nil
	})
}

// Exist checks if the provided triple exists on the store.
func (g *graph) Exist(ctx context.Context, t *triple.Triple) (bool, error) {
	res := false
	err := g.db.View(func(tx *badger.Txn) error {
		if item, err := tx.Get([]byte(graphBucket + "/" + g.id + "/" + idxSPO + "/" + t.String())); item != nil && err == nil {
			res = true
		}
		return nil
	})

	return res, err
}

// Triples pushes to the provided channel all available triples in the graph.
// The function does not return immediately but spawns a goroutine to satisfy
// elements in the channel.
func (g *graph) Triples(ctx context.Context, lo *storage.LookupOptions, trpls chan<- *triple.Triple) error {
	defer close(trpls)
	return g.db.View(func(tx *badger.Txn) error {

		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		opts.PrefetchSize = 1000
		opts.Prefix = []byte(graphBucket + "/" + g.id + "/" + idxSPO + "/")

		it := tx.NewIterator(opts)
		defer it.Close()

		cnt := 0
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()

			err := item.Value(func(v []byte) error {

				if string(v) == "" {
					return nil
				}

				t, err := triple.Parse(string(v), g.lb)
				if err != nil {
					return fmt.Errorf("corrupt index SPO failing to parse %q with error %v", string(v), err)
				}

				if g.shouldAccept(cnt, t, lo) {
					trpls <- t
					cnt++
				}

				return nil
			})

			if err != nil {
				return err
			}
		}
		return nil
	})
}
