{-# LANGUAGE TypeFamilies #-}
module Eclair.Examples.Int1 where

import Control.Monad
import Eclair.Frontend
import Eclair.Backend.Reference


createRef s v =
  transactionally s $ do
    ctx <- getCtx
    let obj = wrap ctx TCounterInt v
    create obj

increment s r =
  transactionally s $ do
    i <- access r
    let i' = incr i
    update r i'

increment3 s r =
  transactionally s $ do
    i <- access r
    let i' = incr $ incr $ incr $ i
    update r i'

decrement2 s r =
  transactionally s $ do
    i <- access r
    let i' = decrBy 2 i
    update r i'

inspect s r =
  transactionally s $ do
  i <- access r
  let v = view i
  return (NF v)

runGeneric s = do
  r <- createRef s 2
  increment3 s r
  decrement2 s r
  increment s r
  increment s r
  v <- inspect s r
  putStrLn ("v: " ++ show v)

run :: IO ()
run = do
  s <- initStore
  runGeneric s
