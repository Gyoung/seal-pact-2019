{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DeriveAnyClass #-}


module Pact.Persist.MPTree
  (
    PValue (..),
    Tbl(..),tbl,
    Tables(..),tbls,
    Db(..),dataTables,
    -- committed,tblType,
    stateRootTable,stateRootTree,
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
import           Seal.DB.MerklePatricia.Internal
import qualified Data.NibbleString                              as N
import           Pos.DB.Rocks.Functions                
import           Pos.Binary.Class (Bi (..), serialize',decodeFull')
import           Pos.DB.Rocks.Types
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
data Tbl k = Tbl {
  _tbl :: MM.MapModifier k PValue,
  _stateRootTable :: StateRoot
  } deriving (Show,Semigroup,Monoid)
makeLenses ''Tbl

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
  _stateRootTree :: StateRoot
  }
makeLenses ''MPtreeDb
instance Default MPtreeDb where def = MPtreeDb def emptyTriePtr

initMPtreeDb :: MPtreeDb
initMPtreeDb = def

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

baseLookup :: (Eq k, Hashable k) => k -> Maybe v
baseLookup _ = undefined

--todo 从mpTree中读取数据
baseLookupM :: (Applicative m, Eq k, Hashable k) => k -> m (Maybe v)
baseLookupM _ = undefined

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
  --value没有，zai
  pv <- case MM.lookup baseLookup t tables of 
          Nothing -> throwDbError $ "writeValue: no such table: "
          Just v  -> case MM.lookup baseLookup k (_tbl v) of 
            Nothing -> throwDbError $ "writeValue: no such table: "
            Just vv -> return vv
  v <- conv $ pv
  return $ (s,) $ (Just v)


tableMPKey :: Table k -> MPKey
tableMPKey (DataTable t) = byteString2TermNibbleString $ sanitize t

sanitize :: TableId -> B.ByteString
sanitize (TableId t) = encodeUtf8 t

-- 判断mptree中是否有table,没有则创建，同时创建一个modifyer，如果有，则直接创建modifyer
createTable_ :: (Hashable k) => Table k -> MPtreeDb -> IO (MPtreeDb,())
-- createTable_ t m = do
--   va <- getTableStateRoot (tableMPKey t) (_stateRootTree m)
--   case va  of 
--     Nothing -> throwDbError $ "writeValue: no such table: "
--     Just t -> throwDbError $ "writeValue: no such table: "
--   return $ (,()) $ m
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

insertMP :: (PactKey k) => Table k -> Tbl k -> MPtreeDb -> MM.MapModifier (Table k) (Tbl k)
insertMP t v m =  MM.insert t v hm
  where hm = view (temp . tblType t . tbls) m
  -- $ view (temp . tblType t . tbls) m      

--初始化mptree,创建tables树
beginTx_ :: MPtreeDb -> IO (MPtreeDb,())
-- beginTx_ s = do
--     let p = set temp (_committed s) s
--     return $ (,()) $ p
beginTx_ s = return $ (,()) $ s

-- todo commit map to MPTree  /tables  /table->k,v
commitTx_ :: MPtreeDb -> IO (MPtreeDb,())
commitTx_ s = undefined
  -- do 
--     let db = _tbls . _dataTables . _temp $ s
--     -- let keys = M.keys db
--     -- k:table value:map
--     st <- forM db $ \(k,v) -> case getTableStateRoot k of 
--        --tables中不存在table,新增
--        Nothing -> addTableValue (M.toList v) emptyTriePtr
--        --tables中存在table,更新 MPVal -> StateRoot
--        Just t -> addTableValue (M.toList v) (StateRoot $ transMaybeMPVal t) 
--     -- map (\(k,v) -> case getTableStateRoot k of 
--     --   --tables中不存在table,新增
--     --   Nothing -> addTableValue (M.toList v) emptyTriePtr
--     --   --tables中存在table,更新 MPVal -> StateRoot
--     --   Just t -> addTableValue (M.toList v) (StateRoot $ transMaybeMPVal t) 
--     --     ) db
--     -- map (\k->case (M.lookup k db) of 
--     --   Nothing -> throwDbError $ "writeValue: no such table: " ++ show t
--     --   Just tb -> 
--     --     ) db
--     -- let (key,value) = db
--     -- map map --循环遍历
--     -- map 
--     -- 遍历db里面都tables和values
--     -- values -> 存入mptree -> 返回rootId -> 存入tables？
--     -- let p = set committed (_temp s) s
--     return $ (,()) $ s 

transMaybeMPVal :: Maybe MPVal -> MPVal
transMaybeMPVal Nothing = ""
transMaybeMPVal (Just v) = v

-- reset db to def
rollbackTx_ :: MPtreeDb -> IO (MPtreeDb,())
rollbackTx_ s = 
  return $ (,()) $ MPtreeDb def (_stateRootTree s)

--从tables中获取table
getTableStateRoot ::(MonadIO m,MonadFail m)=> MPKey -> StateRoot -> m (Maybe MPVal)
getTableStateRoot key sr = do
  nodedbs <- openRocksDB "/seal/contract"
  let mpdb' = MPDB {rdb=nodedbs,stateRoot=sr}
  v <- getKeyVal mpdb' key
  return $ v

-- 更新tables树中table key value
setTableStateRoot :: MPKey -> MPVal -> ()
setTableStateRoot k v = undefined
  -- do
  -- nodedbs <- openRocksDB "/seal/contract"
  -- let mpdb' = MPDB {rdb=nodedbs,stateRoot=(head tablesStateRoot)}
  -- -- mp <- putKeyVal mpdb' k v
  -- void $ putKeyVal mpdb' k v
  -- todo 更新tables stateRoot
  -- (stateRoot mp) : tablesStateRoot 

-- 将table value存入具体的table数中   todo 更新tables树索引
addTableValue :: [(MPKey, MPVal)] -> StateRoot -> StateRoot
addTableValue ls root = undefined
  -- do
  -- nodedbs <- openRocksDB "/seal/contract"
  -- let mpdb' = MPDB {rdb=nodedbs,stateRoot = root}
  -- sts <- forM ls $ \(k,v) -> do
  --   mp <- putKeyVal mpdb' k v
  --   -- todo head mp 是最后一个还是第一个，是否需要reverse
  --   return $ stateRoot mp
  
  -- return sts

  -- map (\(k,v) -> putKeyVal mpdb' k v) ls
  -- return stateRoot mpdb'



-- rootKey /seal/tables 先保存table，再保存tables表？  k - v
-- key:tableName  value:tableRoot
-- key:key value:value
-- saveToMpTreeDb :: String -> k -> v
-- saveToMpTreeDb rootKey =  do
--   nodedbs <- openRocksDB rootKey
--   let rdb' = MPDB {rdb=nodedbs,stateRoot=emptyTriePtr}
--   putKeyVal rdb' k $ serialize' v 


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
