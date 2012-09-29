{-# LANGUAGE TypeFamilies #-}
module Eclair.Examples.Map1 where

import Control.Monad
import Data.Map(Map)
import qualified Data.Map as Map
import Eclair.Frontend
import Eclair.Backend.Reference


createRef s v =
  transactionally s $ do
    ctx <- getCtx
    let obj = wrap ctx (TDict :: TDict (IDict Int (VersionedObject (RCounter Int)))) v
    create obj

inspect s r =
  transactionally s $ do
    i <- access r
    let m = view i
    let v = Map.size m
    return (NF v)

insert s r =
  transactionally s $ do
    m   <- access r
    ctx <- getCtx
    let i  = wrap ctx TCounterInt 1
        m' = updateBinding 5 i m
    update r m'

runGeneric s = do
  r <- createRef s Map.empty
  insert s r
  insert s r
  v <- inspect s r
  putStrLn ("v: " ++ show v)

run :: IO ()
run = do
  s <- initStore
  runGeneric s
