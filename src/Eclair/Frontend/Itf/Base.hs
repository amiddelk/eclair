{-# LANGUAGE TypeFamilies, FlexibleInstances, UndecidableInstances, GADTs #-}
module Eclair.Frontend.Itf.Base where

import Eclair.Frontend.Base


-- | Variant of @IsRoot@ that has a purely functional merging
--   function.
class IsRootPure o where
  joinRootsPure :: o -> o -> o

-- | Automatic conversion of a pure root into a conventional root.
instance IsRootPure o => IsRoot o where
  joinRoots p q = return $ joinRootsPure p q


-- * Common interfaces to objects
--   Note: not all objects provide all these interfaces.

-- | Given an object that refers to another memory space, follow it
--   and access it in the same snapshot.
--   This is a pure function, although it may have internal side effects
--   when it needs to read from the store.
class IsObj o => HasFollow o where
  follow :: (IsStore s, ObjStore o ~ s, Ref s o' ~ ObjType o) => Obj o -> Obj o'

-- | Extracts the plain contents of an object.
class IsObj o => HasView o where
  view :: (IsStore s, ObjStore o ~ s) => Obj o -> ObjType o

-- | Transforms an object by mapping a function over its plain contents.
class IsObj o => HasModify o where
  modify :: (IsStore s, ObjStore o ~ s) => (ObjType o -> ObjType o) -> Obj o -> Obj o

-- | Wraps a plain value into an object.
--   Todo: all objects should be associated in a snapshot, so the implementation
--   of this function should grab the latest snapshot from the Ctx object. Alternatively,
--   we can parameterize it with an explicit snapshot.
class IsObj o => HasWrap o where
  wrap :: (IsStore s, ObjStore o ~ s) => Ctx s -> ObjType o -> Obj o

class (IsObj o, HasView o, HasModify o, HasWrap o) => IsPlain o
instance (IsObj o, HasView o, HasModify o, HasWrap o) => IsPlain o

-- | A dictionary that stores the proof that the object type of o is equal to t.
data ObjTypeDict :: * -> * -> * where
  ObjTypeDict :: (ObjType o ~ t) => ObjTypeDict o t
