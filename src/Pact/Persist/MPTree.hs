{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
-- {-# LANGUAGE DeriveAnyClass #-}


module Pact.Persist.MPTree
  (
    PValue (..),
    Tbl(..),tbl,
    Tables(..),tbls,
    Db(..),dataTables,
    -- committed,tblType,stateRootTree,stateRootTable,
    rootMPDB,
    MPtreeDb(..),temp,
    initMPtreeDb,
    persister
  ) where

-- import qualified Data.Map.Strict as M
import Control.Lens hiding (op)
import Data.Aeson
import Control.Monad.Reader ()
import Control.Monad.State
import Data.Default
import Data.Typeable
import Data.Semigroup (Semigroup)

import Pact.Persist hiding (compileQuery)

import           Seal.DB.MerklePatricia
-- import           Seal.DB.MerklePatricia.Internal
-- import qualified Data.NibbleString                              as N
import           Pos.DB.Rocks.Functions                
-- import           Pos.Binary.Class (Bi (..), serialize',decodeFull')
-- import           Pos.DB.Rocks.Types
import           Seal.DB.MerklePatricia.Utils
import           Universum (MonadFail)
import qualified Data.ByteString   as B
import qualified Pos.Util.Modifier as MM
import Data.Text.Encoding
import           Data.Hashable (Hashable)



-- import Universum((<>))



data PValue = forall a . PactValue a => PValue a
instance Show PValue where show (PValue a) = show a

--MM.Modifyer 保存某张表的修改  
newtype Tbl k = Tbl {
  _tbl :: MM.MapModifier k PValue
  -- ,_tableMPDB :: MPDB
  } deriving (Show,Semigroup,Monoid)
makeLenses ''Tbl

-- instance Show Tbl k where
--   show = ""

--MM.Modifyer 多余？ 只保留每个table自己的modifyer
-- 之前是在内存中，才需要这样做
newtype Tables k = Tables {
  _tbls :: MM.MapModifier (Table k) (Tbl k)
  } deriving (Show,Semigroup,Monoid)
makeLenses ''Tables

-- 多余？
data Db = Db {
  _dataTables :: !(Tables DataKey)
  } deriving (Show)
makeLenses ''Db
instance Default Db where def = Db mempty

tblType :: Table k -> Lens' Db (Tables k)
tblType DataTable {} = dataTables
tblType TxTable {} = undefined --todo 是否会有影响

data MPtreeDb = MPtreeDb {
  -- _committed :: !Db,
  _temp :: !Db,
  _rootMPDB :: MPDB           
  }
makeLenses ''MPtreeDb
-- instance Default MPtreeDb where def = MPtreeDb def emptyTriePtr

initMPtreeDb :: IO MPtreeDb
initMPtreeDb = do
  db <- openRocksDB "/seal/contract"
  let rdb' = MPDB {rdb=db,stateRoot=emptyTriePtr}
  initializeBlank rdb'
  return $ MPtreeDb def rdb'

overM :: s -> Lens' s a -> (a -> IO a) -> IO s
overM s l f = f (view l s) >>= \a -> return (set l a s)
{-# INLINE overM #-}

persister :: Persister MPtreeDb
persister = Persister {
 --insert temp map
  -- createTable = \t s -> fmap (,()) $ overM s (temp . tblType t . tbls) $ \ts -> case M.lookup t ts of
  --     Nothing -> return (M.insert t mempty ts)
  --     Just _ -> throwDbError $ "createTable: already exists: " ++ show t
  -- ,
  -- 判断mptree中是否有table,没有则创建，同时创建一个modifyer，如果有，则直接创建modifyer
  createTable = \t s -> createTable_ t s,
--   beginTx = \_ s -> return $ (,()) $ set temp (_committed s) s
  beginTx = \_ s -> beginTx_ s
  ,
--   commitTx = \s -> return $ (,()) $ set committed (_temp s) s
-- modifyer 值 提交到mptree  modifyer 设置null   delete,insert
  commitTx = \s -> commitTx_ s
  ,
--   rollbackTx = \s -> return $ (,()) $ set temp (_committed s) s
  rollbackTx = \s -> rollbackTx_ s
  ,
  queryKeys = \t kq s -> (s,) . map fst <$> qry t kq s
  ,
  query = \t kq s -> fmap (s,) $ qry t kq s >>= mapM (\(k,v) -> (k,) <$> conv v)
  ,
  -- 新找到表到modifyer，从modifyer readvalue
  -- readValue = \t k s -> fmap (s,) $ traverse conv $ firstOf (temp . tblType t . tbls . ix t . tbl . ix k) s
  readValue = \t k s -> readValue_ t k s
  ,
  --数据库读取表 ->stateroot -> MM.modifyer 
  -- writeValue = \t wt k v s -> fmap (,()) $ overM s (temp . tblType t . tbls) $ \ts -> case M.lookup t ts of
  --     Nothing -> throwDbError $ "writeValue: no such table: " ++ show t
  --     Just tb -> fmap (\nt -> M.insert t nt ts) $ overM tb tbl $ \m -> case (M.lookup k m,wt) of
  --       (Just _,Insert) -> throwDbError $ "Insert: value already at key: " ++ show k
  --       (Nothing,Update) -> throwDbError $ "Update: no value at key: " ++ show k
  --       _ -> return $ M.insert k (PValue v) m
  writeValue = \t wt k v s -> writeValue_ t wt k v s
  ,
  refreshConn = return . (,())
  }

baseLookup :: k -> Maybe v
baseLookup _ = Nothing


--先从modifyer中读取，没有，则从mptree中读取
--table key 
--通过table从tables中找出table
--通过key从table中找出value
--类型转换 MPVal->PValue
readValue_ :: (PactKey k, PactValue v) => Table k -> k -> MPtreeDb -> IO (MPtreeDb,(Maybe v))
readValue_ t k s = do
  --获取tables
  -- let tables = _tbls . _dataTables . _temp $ s
  let tables = view (temp . tblType t . tbls) s
  --通过key，获取对应的value
  --mptree-tables中获取具体的table
  let  baseGetter :: Table k ->  m (Maybe v)
       baseGetter _ = undefined
  mtbl <- MM.lookupM baseGetter t tables
  pv <- case mtbl of 
          --从mptree中读取 
          Nothing          -> throwDbError $ "readValue: no such table: " ++ show t
          Just table  -> do
            let valueGetter :: k -> m (Maybe v)
                valueGetter _ = undefined
            mpv <- MM.lookupM valueGetter k (_tbl table)
            case mpv of 
              Nothing -> throwDbError $ "readValue: no value at key: " ++ show k
              Just kValue -> return kValue
  v <- conv $ pv
  return $ (s,) $ (Just v)
  -- where 
  --   baseGetter :: k ->  (Maybe v)
  --   baseGetter k = undefined

--从mptree中读取数据
-- table k, k -> maybe v
-- readFromMPtree :: (PactKey k) => Table k -> k -> MPtreeDb -> IO PValue
-- readFromMPtree t k s = do
--   let mpval = tableKey2MPKey t
--   -- Maybe MPVal --MPVal 2 MpKey
--   tid <- getMPtreeValue mpval (_rootMPDB s)
--   kva <- case tid of 
--            Nothing -> throwDbError $ "readValue: no such table: " ++ show t 
--            Just ta -> do
--              tval <- getMPtreeValue (pactKey2MPKey k) (mpVal2MPDB ta)
--              case tval of 
--                Nothing -> throwDbError $ "readValue: no value at  key: " ++ show k 
--                Just va -> return va
--   return $ mpVal2PValue kva           

-- instance PactValue MPVal
  
-- mpVal2PValue :: MPVal -> PValue
-- mpVal2PValue p = undefined

-- pactKey2MPKey :: PactKey k => k -> MPKey
-- pactKey2MPKey _ = undefined

mpVal2MPDB :: MPVal -> MPDB
mpVal2MPDB _ = undefined

tableKey2MPKey :: Table k -> MPKey
tableKey2MPKey (DataTable t) = byteString2TermNibbleString $ sanitize t
tableKey2MPKey (TxTable t) = byteString2TermNibbleString $ sanitize t


sanitize :: TableId -> B.ByteString
sanitize (TableId t) = encodeUtf8 t

-- 判断mptree中是否有table,没有则创建，同时创建一个modifyer，如果有，则直接创建modifyer
createTable_ :: (Hashable k,Eq k) => Table k -> MPtreeDb -> IO (MPtreeDb,())
-- 直接在mptree中创建table  
createTable_ t s = fmap (,()) $ overM s (temp . tblType t . tbls) $ \ts -> case MM.lookup baseLookup t ts of
  Nothing -> return (MM.insert t mempty ts)
  Just _ -> throwDbError $ "createTable: already exists: " ++ show t

writeValue_ :: (PactKey k, PactValue v) => Table k -> WriteType -> k -> v -> MPtreeDb -> IO (MPtreeDb,())
writeValue_ t wt k v s =  fmap (,()) $ overM s (temp . tblType t . tbls) $ \ts -> case MM.lookup baseLookup t ts of
  Nothing -> throwDbError $ "writeValue: no such table: " ++ show t
  Just tb -> fmap (\nt -> MM.insert t nt ts) $ overM tb tbl $ \m -> case (MM.lookup baseLookup k m,wt) of
    (Just _,Insert) -> throwDbError $ "Insert: value already at key: " ++ show k
    (Nothing,Update) -> throwDbError $ "Update: no value at key: " ++ show k
    _ -> return $ MM.insert k (PValue v) m


--初始化mptree,创建tables树
beginTx_ :: MPtreeDb -> IO (MPtreeDb,())
-- beginTx_ s = do
--     let p = set temp (_committed s) s
--     return $ (,()) $ p
beginTx_ s = return $ (,()) $ s

tablesSchema :: [(Table DataKey,Tbl DataKey)]
tablesSchema = []

-- todo commit map to MPTree  /tables  /table->k,v
commitTx_ :: MPtreeDb -> IO (MPtreeDb,())
commitTx_ s = do
  let tables = _tbls . _dataTables . _temp $ s
  let    ls  = MM.toList tablesSchema tables
  root <- setMptreeTables ls (_rootMPDB s)
  -- 更新最外层StateRoot
  let ss = set rootMPDB root s
  return $ (,()) $ ss

tableSchema :: [(DataKey,PValue)]
tableSchema = []

--将tables插入mptree
setMptreeTables :: [(Table DataKey,Tbl DataKey)] -> MPDB -> IO MPDB
setMptreeTables ((k,v):xs) mpdb = do
  let mpKey = tableKey2MPKey k
  let values = MM.toList tableSchema (_tbl v)
  tid <- getMPtreeValue mpKey mpdb
  tsr <- case tid of 
    Nothing -> setMPtreeValues values (MPDB {rdb=(rdb mpdb),stateRoot=emptyTriePtr})
    Just tv -> setMPtreeValues values (mpVal2MPDB tv)
  nst <- setMPtreeValue mpKey (mpdb2MPval tsr) mpdb
  setMptreeTables xs nst
setMptreeTables [] mpdb = return mpdb


-- 将modifyer中的值插入table mptree
setMPtreeValues :: [(DataKey,PValue)] -> MPDB -> IO MPDB
setMPtreeValues ((k,v):xs) mpdb = do
  let mpKey = dataKey2MPKey k
  let mpVal = pValue2MPVal v
  rs <- setMPtreeValue mpKey mpVal mpdb
  --更新stateroot
  setMPtreeValues xs rs
setMPtreeValues [] mpdb = return mpdb  

dataKey2MPKey :: DataKey -> MPKey
dataKey2MPKey _ = undefined

pValue2MPVal :: PValue -> MPVal
pValue2MPVal _ = undefined

mpdb2MPval :: MPDB -> MPVal
mpdb2MPval _ = undefined

-- transMaybeMPVal :: Maybe MPVal -> MPVal
-- transMaybeMPVal Nothing = ""
-- transMaybeMPVal (Just v) = v

-- reset db to def
rollbackTx_ :: MPtreeDb -> IO (MPtreeDb,())
rollbackTx_ s = 
  return $ (,()) $ MPtreeDb def (_rootMPDB s)

-- MPtree中获取值
getMPtreeValue ::(MonadIO m,MonadFail m)=> MPKey -> MPDB -> m (Maybe MPVal)
getMPtreeValue key mpdb = do
  v <- getKeyVal mpdb key
  return $ v

-- MPtree中插入新的值
setMPtreeValue :: MPKey -> MPVal -> MPDB -> IO MPDB
setMPtreeValue k v mpdb = do
  st <- putKeyVal mpdb k v
  return $ st


--queryKeys 先从map里面查，再从mptree里面查？

-- compileQuery :: PactKey k => Maybe (KeyQuery k) -> (k -> Bool)
-- compileQuery Nothing = const True
-- compileQuery (Just kq) = compile kq
--   where
--     compile (KQKey cmp k) = (`op` k)
--       where op = case cmp of
--               KGT -> (>)
--               KGTE -> (>=)
--               KEQ -> (==)
--               KNEQ -> (/=)
--               KLT -> (<)
--               KLTE -> (<=)
--     compile (KQConj l o r) = conj o <$> compile l <*> compile r
--     conj AND = (&&)
--     conj OR = (||)
-- {-# INLINE compileQuery #-}

qry :: Table k -> Maybe (KeyQuery k) -> MPtreeDb -> IO [(k,PValue)]
-- qry t kq s = case firstOf (temp . tblType t . tbls . ix t . tbl) s of
--   Nothing -> throwDbError $ "query: no such table: " ++ show t
--   Just m -> return $ filter (compileQuery kq . fst) $ M.toList m
qry _ _ _ = undefined

{-# INLINE qry #-}


conv :: PactValue v => PValue -> IO v
conv (PValue v) = case cast v of
  Nothing -> throwDbError $ "Failed to reify DB value: " ++ show v
  Just s -> return s
{-# INLINE conv #-}




_test :: IO ()
_test = do
  let e :: MPtreeDb = MPtreeDb {_temp=def,_rootMPDB=undefined}
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
