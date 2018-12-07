{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}

module Pact.Persist.MPTree
  (
    PValue (..),
    Tbl(..),tbl,
    Tables(..),tbls,tblType,
    Db(..),dataTables,
    MPtreeDb(..),committed,temp,
    initMPtreeDb,
    persister
  ) where

import qualified Data.Map.Strict as M
import Control.Lens hiding (op)
import Data.Aeson
import Control.Monad.Reader ()
import Control.Monad.State
import Data.Default
import Data.Typeable
import Data.Semigroup (Semigroup)

import Pact.Persist hiding (compileQuery)

data PValue = forall a . PactValue a => PValue a
instance Show PValue where show (PValue a) = show a


newtype Tbl k = Tbl {
  _tbl :: M.Map k PValue
  } deriving (Show,Semigroup,Monoid)
makeLenses ''Tbl

newtype Tables k = Tables {
  _tbls :: M.Map (Table k) (Tbl k)
  } deriving (Show,Semigroup,Monoid)
makeLenses ''Tables


data Db = Db {
  _dataTables :: !(Tables DataKey)
  } deriving (Show)
makeLenses ''Db
instance Default Db where def = Db mempty

tblType :: Table k -> Lens' Db (Tables k)
tblType DataTable {} = dataTables
tblType TxTable {} = undefined

data MPtreeDb = MPtreeDb {
  _committed :: !Db,
  _temp :: !Db
  }
makeLenses ''MPtreeDb
instance Default MPtreeDb where def = MPtreeDb def def

initMPtreeDb :: MPtreeDb
initMPtreeDb = def

overM :: s -> Lens' s a -> (a -> IO a) -> IO s
overM s l f = f (view l s) >>= \a -> return (set l a s)
{-# INLINE overM #-}

persister :: Persister MPtreeDb
persister = Persister {
 --insert temp map
  createTable = \t s -> fmap (,()) $ overM s (temp . tblType t . tbls) $ \ts -> case M.lookup t ts of
      Nothing -> return (M.insert t mempty ts)
      Just _ -> throwDbError $ "createTable: already exists: " ++ show t
  ,
--   beginTx = \_ s -> return $ (,()) $ set temp (_committed s) s
  beginTx = \_ s -> beginTx_ s
  ,
--   commitTx = \s -> return $ (,()) $ set committed (_temp s) s
  commitTx = \s -> commitTx_ s
  ,
--   rollbackTx = \s -> return $ (,()) $ set temp (_committed s) s
  rollbackTx = \s -> rollbackTx_ s
  ,
  queryKeys = \t kq s -> (s,) . map fst <$> qry t kq s
  ,
  query = \t kq s -> fmap (s,) $ qry t kq s >>= mapM (\(k,v) -> (k,) <$> conv v)
  ,
  readValue = \t k s -> fmap (s,) $ traverse conv $ firstOf (temp . tblType t . tbls . ix t . tbl . ix k) s
  ,
  writeValue = \t wt k v s -> fmap (,()) $ overM s (temp . tblType t . tbls) $ \ts -> case M.lookup t ts of
      Nothing -> throwDbError $ "writeValue: no such table: " ++ show t
      Just tb -> fmap (\nt -> M.insert t nt ts) $ overM tb tbl $ \m -> case (M.lookup k m,wt) of
        (Just _,Insert) -> throwDbError $ "Insert: value already at key: " ++ show k
        (Nothing,Update) -> throwDbError $ "Update: no value at key: " ++ show k
        _ -> return $ M.insert k (PValue v) m
  ,
  refreshConn = return . (,())
  }

beginTx_ :: MPtreeDb -> IO (MPtreeDb,())
beginTx_ s = do
    let p = set temp (_committed s) s
    return $ (,()) $ p

-- todo commit map to MPTree
commitTx_ :: MPtreeDb -> IO (MPtreeDb,())
commitTx_ s = do 
    let p = set committed (_temp s) s
    return $ (,()) $ p 

-- todo clean the map
rollbackTx_ :: MPtreeDb -> IO (MPtreeDb,())
rollbackTx_ s = do
    let p = set temp (_committed s) s
    return $ (,()) $ p

--queryKeys 先从map里面查，再从mptree里面查？

compileQuery :: PactKey k => Maybe (KeyQuery k) -> (k -> Bool)
compileQuery Nothing = const True
compileQuery (Just kq) = compile kq
  where
    compile (KQKey cmp k) = (`op` k)
      where op = case cmp of
              KGT -> (>)
              KGTE -> (>=)
              KEQ -> (==)
              KNEQ -> (/=)
              KLT -> (<)
              KLTE -> (<=)
    compile (KQConj l o r) = conj o <$> compile l <*> compile r
    conj AND = (&&)
    conj OR = (||)
{-# INLINE compileQuery #-}

qry :: PactKey k => Table k -> Maybe (KeyQuery k) -> MPtreeDb -> IO [(k,PValue)]
qry t kq s = case firstOf (temp . tblType t . tbls . ix t . tbl) s of
  Nothing -> throwDbError $ "query: no such table: " ++ show t
  Just m -> return $ filter (compileQuery kq . fst) $ M.toList m
{-# INLINE qry #-}


conv :: PactValue v => PValue -> IO v
conv (PValue v) = case cast v of
  Nothing -> throwDbError $ "Failed to reify DB value: " ++ show v
  Just s -> return s
{-# INLINE conv #-}




_test :: IO ()
_test = do
  let e :: MPtreeDb = def
  let p = persister
      dt = DataTable "data"
      tt = TxTable "tx"
      run f = do
        s <- get
        (s',r) <- liftIO (f s)
        put s'
        return r
  (`evalStateT` e) $ do
    run $ beginTx p True
    run $ createTable p dt
    run $ createTable p tt
    run $ commitTx p
    run $ beginTx p True
    run $ writeValue p dt Insert "stuff" (String "hello")
    run $ writeValue p dt Insert "tough" (String "goodbye")
    run $ writeValue p tt Write 1 (String "txy goodness")
    run $ writeValue p tt Insert 2 (String "txalicious")
    run $ commitTx p
    run (readValue p dt "stuff") >>= (liftIO . (print :: Maybe Value -> IO ()))
    run (query p dt (Just (KQKey KEQ "stuff"))) >>=
      (liftIO . (print :: [(DataKey,Value)] -> IO ()))
    run (queryKeys p dt (Just (KQKey KGTE "stuff"))) >>= liftIO . print
    run (query p tt (Just (KQKey KGT 0 `kAnd` KQKey KLT 2))) >>=
      (liftIO . (print :: [(TxKey,Value)] -> IO ()))
    run $ beginTx p True
    run $ writeValue p tt Update 2 (String "txalicious-2!")
    run (readValue p tt 2) >>= (liftIO . (print :: Maybe Value -> IO ()))
    run $ rollbackTx p
    run (readValue p tt 2) >>= (liftIO . (print :: Maybe Value -> IO ()))
