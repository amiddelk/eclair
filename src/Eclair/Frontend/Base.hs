{-# LANGUAGE TypeFamilies, DeriveDataTypeable, FlexibleInstances #-}
module Eclair.Frontend.Base 
  ( TransactM
  , Ctx, ctxStore, ctxTrans
  , IsStore, Trans, Ref
  , openTransaction, abortTransaction, commitTransaction
  , accessSpace, allocSpace, updateSpace
  , onTransactionRestart
  , IsRoot
  , IsObj
  , ObjStore, ObjType, Obj, ObjRef
  , objValue, objCtx, wrapRef
  , TxnResult, NF(NF), exportResult
  , TransactionRestart(RequireRestart)
  , transactionally, performAsyncPure
  , wrapObj, getCtx
  , publish, create, update, access
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
--   is created inside the transaction in which the pure objects reside
--   (accessible only by the transaction).
class Eq s => IsStore s where
  type Trans s :: *
  type Ref s :: * -> *

  openTransaction   :: s -> IO (Trans s)
  abortTransaction  :: s -> Trans s -> IO ()
  commitTransaction :: s -> Trans s -> IO ()

  -- | A handler that is called prior to restarting a transaction. Because
  --   transactions are pure, a restart can only have a different
  --   outcome when the store has been modified in the mean time. This handler
  --   can e.g. be used to let the caller wait until the store is
  --   changed in the background.
  onTransactionRestart :: s -> IO ()
  onTransactionRestart _ = return ()  -- default implementation


-- | A handle to the store.
data Ctx s = Ctx
  { ctxStore :: s
  , ctxTrans :: Trans s
  , ctxChan  :: !( Chan PureCommand )
  }


-- | A special class of objects that are roots of a space.
--   These roots must be a commutative semi-monoid (or a
--   commutative semi-group). The joining operation may use
--   side effects (it's in the IO monad).
class IsObj o => IsRoot o where
  -- | Obtains the root object from the space identified by the reference in the given snapshot.
  accessSpace :: (IsStore s, s ~ ObjStore o) => s -> Trans s -> Ref s o -> IO o

  -- | Allocates a new space with the given root object as it contents and returns a reference to it.
  allocSpace :: (IsStore s, s ~ ObjStore o) => IsRoot o => s -> Trans s -> o -> IO (Ref s o)

  -- | Stores the object @o@ (in the given snapshot) as root in the space identified by the reference.
  updateSpace :: (IsStore s, s ~ ObjStore o) => s -> Trans s -> Ref s o -> o -> IO ()

  -- | Merges the contents of two spaces.
  -- joinRoots :: o -> o -> IO o


-- | An object is a sharable node in the data-graph of a memory
--   space. Its implementation depends on the actual store.
class IsObj o where
  type ObjType o  :: *  -- the exposed type of the object
  type ObjStore o :: *  -- the associated store of the object

-- | Wrapper around an object @o@ that stores some information
--   about the underlying object @o@, such as the snapshot from
--   which @o@ stems from.
data Obj o = Obj
  { objValue :: !o
  , objCtx   :: !(Ctx (ObjStore o))
  , objRef   :: !(Maybe (Ref (ObjStore o) o))
  }

-- | Wrapper around a reference that can be moved out of the
--   transaction and keeps track of the store it was created in.
data ObjRef o = ObjRef
  { refBackend :: !( Ref (ObjStore o) o )
  , refStore   :: !( ObjStore o )
  }

-- | ObjRefs can be exported out of a transaction
instance TxnResult (ObjRef o) where
  exportResult ref = do
    evaluate ref
    return ref

assertSameStore :: (IsStore s, s ~ ObjStore o) => Ctx s -> ObjRef o -> IO ()
assertSameStore ctx ref =
  unless (ctxStore ctx == refStore ref) $
    error "a reference must be used with the same store instance that created it."


-- * Values exportable from a transaction

-- | An interface for preparing values, that are the result of
--   a transaction, for leaving the scope of a transaction.
class TxnResult a where
  exportResult :: a -> IO a

instance TxnResult () where
  exportResult = evaluate

instance TxnResult Int where
  exportResult = evaluate

instance TxnResult Bool where
  exportResult = evaluate

-- | A wrapper around results that can be evaluated to normal form.
--   These results can be exported from the transaction.
--
--   Above are also some direct instances of TxnResult for common cases.
newtype NF a = NF a
  deriving (Eq, Ord, Show, Typeable)

-- | Fully-evaluated data cannot refer to lazy values in the transaction,
--   thus are safe to be returned by the transaction.
instance NFData a => TxnResult (NF a) where
  exportResult r =
    case r of
      NF a -> do
        evaluate $ rnf a
        return r


-- * The toplevel function.

-- | This exception can be thrown to force the restart of a transaction,
--   in either the pure or monadic code.
data TransactionRestart = RequireRestart
  deriving (Eq, Show, Typeable)

instance Exception TransactionRestart

-- | @transactionally s f@ exposes the operations on @s@ to @f@, which
-- are run in the @TransactM@ monad. The transaction gives access to lazily created
-- snapshots of the store.
transactionally :: (TxnResult a, IsStore s) => s -> TransactM s a -> IO a
transactionally store txn = loop where
  loop = handle (\RequireRestart -> onTransactionRestart store >> loop) step
  step = bracketOnError (openTransaction store) (abortTransaction store) $ \trans -> do
    chan     <- newChan
    let ctx = Ctx { ctxStore = store,   ctxTrans = trans
                  , ctxChan  = chan }
    result <- newEmptyMVar
    flip finally (writeChan chan CommDone) $ do
      tId <- myThreadId
      void $ forkIO $ consumePure tId chan
      res <- runTransM ctx txn
      commitTransaction store trans
      return res

-- | Consumes the commands that are enqueued to the channel.
consumePure :: ThreadId -> Chan PureCommand -> IO ()
consumePure tId chan = wrap where
  wrap = handleAny (throwTo tId) loop
  loop = do
    act <- readChan chan
    case act of
      CommDo m      -> m >> loop
      CommDone      -> return ()

-- | Runs the transaction, thereby causing evaluation.
runTransM :: TxnResult a => Ctx s -> TransactM s a -> IO a
runTransM ctx m = do
  res <- runReaderT (unTransactM m) ctx
  exportResult res

-- | Commands that can be passed to the backend.
data PureCommand
  = CommDo !( IO () )
  | CommDone

handleAny :: (SomeException -> IO a) -> IO a -> IO a
handleAny = handle

catchAny :: IO a -> (SomeException -> IO a) -> IO a
catchAny = Control.Exception.catch

-- | Executes some supposedly commands on the backend that
--   can be considered pure with respect to the current
--   transaction. See @performAsync@.
{-# NOINLINE performAsyncPure #-}
performAsyncPure :: Ctx s -> ( (a -> IO ()) -> IO () ) -> a
performAsyncPure ctx f = unsafePerformIO (performAsync ctx f)

-- | Performs some action @f@ on the backend asynchronously
--   by putting it in the command queue. Any exception
--   raised by the command handler will be caught and
--   reraised when the result is needed.
performAsync :: Ctx s -> ( (a -> IO ()) -> IO () ) -> IO a
performAsync ctx f = do
  var <- newEmptyMVar  -- future for final result
  let cont a = putMVar var a
  let comm = catchAny (f cont) (cont . throw)
  writeChan (ctxChan ctx) (CommDo comm)
  takeMVar var

-- | Wraps a backend object @o@ into a frontend object @Obj o@.
wrapObj :: (IsObj o, IsStore s, ObjStore o ~ s) => Ctx s -> Maybe (Ref s o) -> o -> Obj o
wrapObj ctx ref o = Obj
  { objValue = o
  , objCtx   = ctx
  , objRef   = ref
  }

-- | Wraps a backend reference into a frontend reference.
wrapRef :: (IsObj o, IsStore s, ObjStore o ~ s) => Ctx s -> Ref s o -> ObjRef o
wrapRef ctx ref = ObjRef
  { refBackend = ref
  , refStore   = ctxStore ctx
  }


-- * Operations in the transaction monad

-- | Obtains the current context.
getCtx :: TransactM s (Ctx s)
getCtx = TransactM ask

-- | Publishes the object, which either destructively updates the
--   given memory space or creates a new one. It returns the
--   reference to the memory space.
publish :: (IsStore s, IsRoot o, s ~ ObjStore o) =>
             Maybe (ObjRef o) -> Obj o -> TransactM s (ObjRef o)
publish mbRef obj =
  let ctx   = objCtx obj
      val   = objValue obj
      store = ctxStore ctx
      trans = ctxTrans ctx
  in TransactM $ liftIO $
       case mbRef of
         Nothing  -> do
           ref <- allocSpace store trans val
           return (wrapRef ctx ref)
         Just ref -> do
           assertSameStore ctx ref
           let backendRef = refBackend ref
           void $ updateSpace store trans backendRef val 
           return ref

-- | Creates a new memory space with the given object as root.
create :: (IsStore s, IsRoot o, s ~ ObjStore o) => Obj o -> TransactM s (ObjRef o)
create = publish Nothing

-- | Destructively updates a memory space.
update :: (IsStore s, IsRoot o, s ~ ObjStore o) => ObjRef o -> Obj o -> TransactM s ()
update ref = void . publish (Just ref)

-- | In the current transaction, creates a (local) snapshot and opens the
--   referenced space in it, giving the object that forms the root of its
--   contents.
access :: (IsStore s, IsRoot o, s ~ ObjStore o) => ObjRef o -> TransactM s (Obj o)
access ref = TransactM $ do
  ctx <- ask
  liftIO $ do
    assertSameStore ctx ref
    let store      = ctxStore ctx
        trans      = ctxTrans ctx
        backendRef = refBackend ref
    obj <- accessSpace store trans backendRef
    return $ wrapObj ctx (Just backendRef) obj
