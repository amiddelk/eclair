{-# LANGUAGE TypeFamilies, DeriveDataTypeable #-}
module Eclair.Frontend.Base 
  ( TransactM
  , Ctx, ctxStore, ctxTrans
  , IsStore, Trans, Snap, Ref
  , openTransaction, abortTransaction, commitTransaction
  , createSnapshot, disposeSnapshot
  , accessSpace, allocSpace, storeSpace
  , onTransactionRestart
  , IsRoot, joinRoots
  , IsObj
  , ObjStore, ObjType, Obj
  , objValue, objCtx, objSnap
  , TransactionRestart(RequireRestart)
  , transactionally, onBackend, onBackendPure, wrapObj, getCtx
  , publish, create, store, access
  ) where

import Control.Applicative
import Control.Concurrent
import Control.Concurrent.STM
import Control.DeepSeq
import Control.Exception
import Control.Monad
import Control.Monad.Fix
import Control.Monad.Reader
import Control.Monad.Trans
import Data.HashTable
import Data.IORef
import Data.Typeable
import System.IO.Unsafe
import System.Mem.Weak


-- *  The TransactM monad, which can be used to transactionlly perform some
--    operations on (some snapshot of) the store.

-- | The TransactM-monad gives access to the @Ctx@ object. The bind operator
--   is strict. It gives the underlying implementation access to the IO monad.
newtype TransactM s a = TransactM { unTransactM :: ReaderT (Ctx s) IO a }

instance Monad (TransactM s) where
  (TransactM m) >>= f = TransactM (m >>= \a -> unTransactM (f a))
  (TransactM a) >> (TransactM b) = TransactM (a >> b)
  return a = TransactM (return a)
  fail s = TransactM (fail s)

instance Functor (TransactM s) where
  fmap f (TransactM m) = TransactM (fmap f m)

instance MonadFix (TransactM s) where
  mfix f = TransactM (mfix (\a -> unTransactM (f a)))

instance Applicative (TransactM s) where
  pure  = return
  (<*>) = ap
  (*>)  = (>>)


-- * The store.

-- | A store of type s. This class defines the interface of the store, and
--   key concepts such as transactions and snapshots.
--
--   The store contains memory spaces: such a space is a mutable container
--   of pure objects. A space is identified by a @Ref@, which is needed for
--   accessing and updating the space.
--
--   A transaction @Trans@ represents a unit of accesses and updates of the spaces
--   in the store. When a space is accessed to obtain its contents, a snapshot
--   @Snap@ is created inside the transaction in which the pure objects reside
--   (accessible only by the transaction).
class IsStore s where
  type Trans s :: *
  type Snap s :: *
  type Ref s :: * -> *

  openTransaction   :: s -> IO (Trans s)
  abortTransaction  :: s -> Trans s -> IO ()
  commitTransaction :: s -> Trans s -> IO ()

  createSnapshot    :: s -> Trans s -> Maybe (Snap s) -> IO (Snap s)
  disposeSnapshot   :: s -> Trans s -> Snap s -> IO ()

  -- | Obtains the root object from the space identified by the reference in the given snapshot.
  accessSpace       :: s -> Trans s -> Snap s -> Ref s o -> (o -> IO ()) -> IO ()

  -- | Allocates a new space with the given root object as it contents and returns a reference to it.
  allocSpace        :: IsRoot o => s -> Trans s -> Snap s -> o -> (Ref s o -> IO ()) -> IO ()

  -- | Stores the object @o@ (in the given snapshot) as root in the space identified by the reference.
  storeSpace        :: IsRoot o => s -> Trans s -> Snap s -> Ref s o -> o -> (() -> IO ()) -> IO ()

  -- | A handler that is called prior restarting a transaction. As
  --   transactions are pure, a restart can only have a different
  --   outcome when the store is modified in between. This handler
  --   can e.g. be used to let the caller wait until the store is
  --   changed in the background.
  onTransactionRestart :: s -> IO ()
  onTransactionRestart _ = return ()  -- default implementation


-- | A context is a handle to a store, a transaction,
--   and snapshots created in the transaction. The snapshots in
--   @ctxSnaps@ are in newest-first order.
--
--   A contexts provides a queue for commands to execute
--   on the store. These commands may be side-effectful
--   operations. However, commands that are queued from
--   pure code must either preserve referential transparancy
--   (e.g. reading from the store) or throw a retry or unhandled
--   exception.
--
--   Note: @ctxSnap@ is a reference to the latest snapshot. This
--   may not be the snapshot in which some object was created
--   at the time an operation on it is evaluated. The @objSnap@
--   field of an @Obj@ points to the right snapshot instead.
data Ctx s = Ctx
  { ctxStore   :: s
  , ctxTrans   :: Trans s
  , ctxSnap    :: !( IORef (Maybe (Snap s)) )
  , ctxSnaps   :: !( IORef [Weak (Snap s)] )
  , ctxChan    :: !( Chan StoreCommand )
  }


-- | A special class of objects that are roots of a space.
--   These roots must be a commutative semi-monoid (or a
--   commutative semi-group). The joining operation may use
--   side effect (it's in the IO monad).
class IsRoot o where
  -- | Merges the contents of two spaces.
  joinRoots :: o -> o -> IO o


-- | An object is a sharable node in the data-graph of a memory
--   space. Its implementation depends on the actual store.
class IsObj o where
  type ObjType o  :: *  -- the exposed type of the object
  type ObjStore o :: *  -- the associated store of the object

-- | Wrapper around an object @o@ that stores some information
--   about the underlying object @o@, such as the snapshot from
--   which @o@ comes from.
data Obj o = Obj
  { objValue :: !o
  , objCtx   :: !(Ctx (ObjStore o))
  , objSnap  :: !(Snap (ObjStore o))
  }


-- * The toplevel function.

-- | This exception can be thrown to force the transaction
--   to restart, in either the pure or monadic code.
data TransactionRestart = RequireRestart
  deriving (Eq, Show, Typeable)

instance Exception TransactionRestart

-- | @transactionally s f@ exposes the operations on @s@ to @f@, which
-- are run in the tran monad. The transaction gives access to a lazy
-- snapshot of the storage. To prevent the result value @a@ to refer
-- to a unevaluated part of the snapshot, the result is evaluated to
-- normal form before closing the transaction, hence the requirement on
-- @NFData@.
transactionally :: (NFData a, IsStore s) => s -> TransactM s a -> IO a
transactionally store m = loop where
  loop = handle (\RequireRestart -> onTransactionRestart store >> loop) step
  step = bracketOnError (openTransaction store) (abortTransaction store) $ \trans -> do
    chan  <- newChan
    snaps <- newIORef []
    snap  <- newIORef Nothing
    let ctx = Ctx { ctxStore = store, ctxTrans = trans
                  , ctxSnap = snap, ctxSnaps = snaps
                  , ctxChan = chan }
    var <- newEmptyMVar
    flip finally (disposeSnapshots ctx >> consumeCleanup chan) $
      bracketOnError (forkIO $ produce var ctx m) killThread $ \_ -> do
        consume chan
        res <- takeMVar var
        commitTransaction store trans
        return res

-- | Disposes the live snapshots in the reversed order of their creation.
--   Must be executed on the main thread.
disposeSnapshots :: IsStore s => Ctx s -> IO ()
disposeSnapshots ctx = do
  snaps <- readIORef $ ctxSnaps ctx
  let store = ctxStore ctx
  let trans = ctxTrans ctx
  forM_ snaps finalize
  writeIORef (ctxSnaps ctx) []
  writeIORef (ctxSnap ctx) Nothing

-- | Creates a new snapshot in the transaction and makes it the current.
--   Non-current transactions are disposed when no objects refer to it
--   anymore, or at the latest at the end of the transaction.
pushSnapshot :: IsStore s => Ctx s -> IO (Snap s)
pushSnapshot ctx = do
  let store = ctxStore ctx
  let trans = ctxTrans ctx
  let varSnap = ctxSnap ctx
  let chan = ctxChan ctx
  mbSnap <- readIORef varSnap
  bracketOnError (createSnapshot store trans mbSnap) (disposeSnapshot store trans) $ \snap -> do
    writeIORef varSnap $! Just snap
    w <- mkWeakPtr snap $ Just $ writeChan chan $! CommCleanup $ disposeSnapshot store trans snap
    let varSnaps = ctxSnaps ctx
    snaps <- readIORef varSnaps
    let snaps' = w : snaps
    writeIORef varSnaps $! snaps'
    return snap

-- | Runs the transaction, thereby producing commands (internal function).
produce :: NFData a => MVar a -> Ctx s -> TransactM s a -> IO ()
produce var ctx m =
  let chan = ctxChan ctx in
  handle (\e -> writeChan chan $! CommFailed e) $ do
    res <- runReaderT (unTransactM m) ctx
    evaluate $ rnf res
    putMVar var res
    writeChan chan CommDone

-- | Consumes backend commands and performs them (internal function).
consume :: Chan StoreCommand -> IO ()
consume chan = loop where
  loop = do
    act <- readChan chan
    case act of
      CommDo m      -> m >> loop
      CommCleanup m -> m >> loop
      CommDone      -> return ()
      CommFailed e  -> throw e

-- | Variant of @consume@ that performs only pending cleanup commands,
--   and throws away the others.
consumeCleanup :: Chan StoreCommand -> IO ()
consumeCleanup chan = loop where
  loop = do
    act <- readChan chan
    case act of
      CommCleanup m -> m `finally` loop
      _             -> loop

-- | Commands that can be passed to the backend.
data StoreCommand
  = CommDo !( IO () )
  | CommDone
  | CommFailed !SomeException
  | CommCleanup !( IO () )

-- | Executes some supposedly commands on the backend that
--   can be considered pure with respect to the current
--   transaction. See @onBackend@.
{-# NOINLINE onBackendPure #-}
onBackendPure :: Ctx s -> ( (a -> IO ()) -> IO () ) -> a
onBackendPure ctx f = unsafePerformIO (onBackend ctx f)

-- | Performs some action @f@ on the backend. The action
--   receives a continuation that it must execute to
--   communicate its results back to the caller.
--   Note: failure to do so will hang the program.
onBackend :: Ctx s -> ( (a -> IO ()) -> IO () ) -> IO a
onBackend ctx f = do
  var <- newEmptyMVar
  let cont a = putMVar var a
  let comm = f cont
  writeChan (ctxChan ctx) (CommDo comm)
  takeMVar var

wrapObj :: (IsObj o, IsStore s, ObjStore o ~ s) => Ctx s -> Snap s -> o -> Obj o
wrapObj ctx snap o = Obj
  { objValue = o
  , objCtx   = ctx
  , objSnap  = snap
  }


-- * Operations in the transaction monad

-- | Obtains the current context.
getCtx :: TransactM s (Ctx s)
getCtx = TransactM ask

-- | Publishes the object, which either destructively updates the
--   given memory space or creates a new one. It returns the
--   reference to the memory space.
publish :: (IsStore s, IsRoot o, IsObj o, s ~ ObjStore o) => Maybe (Ref s o) -> Obj o -> TransactM s (Ref s o)
publish mbRef obj =
  let ctx   = objCtx obj
      snap  = objSnap obj
      val   = objValue obj
      store = ctxStore ctx
      trans = ctxTrans ctx
  in TransactM $ liftIO $ onBackend ctx $ \k ->
       case mbRef of
         Nothing  -> allocSpace store trans snap val k
         Just ref -> storeSpace store trans snap ref val $ const $ k ref

-- | Creates a new memory space with the given object as root.
create :: (IsStore s, IsRoot o, IsObj o, s ~ ObjStore o) => Obj o -> TransactM s (Ref s o)
create = publish Nothing

-- | Destructively updates a memory space.
store :: (IsStore s, IsRoot o, IsObj o, s ~ ObjStore o) => Ref s o -> Obj o -> TransactM s ()
store ref = void . publish (Just ref)

-- | In the current transaction, creates a (local) snapshot and opens the
--   referenced space in it, giving the object that forms the root of its
--   contents.
access :: (IsStore s, IsObj o, s ~ ObjStore o) => Ref s o -> TransactM s (Obj o)
access ref = TransactM $ do
  ctx <- ask
  liftIO $ onBackend ctx $ \k -> do
    snap <- pushSnapshot ctx
    let store = ctxStore ctx
        trans = ctxTrans ctx
    accessSpace store trans snap ref (k . wrapObj ctx snap)
