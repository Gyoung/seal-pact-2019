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
    Tbl(..),tbl,
    Tables(..),tbls,
    Db(..),dataTables,txTables,
    rootMPDB,tableStateRoot,
    MPtreeDb(..),temp,
    initMPtreeDb,_test,
    persister
  ) where

import Control.Lens hiding (op)
import Data.Aeson
import Control.Monad.Reader ()
import Control.Monad.State
import Data.Default
-- import Data.Typeable
import Data.Semigroup (Semigroup)

import Pact.Persist hiding (compileQuery)

import           Seal.DB.MerklePatricia
import           Pos.DB.Rocks.Functions                
import           Pos.DB.Rocks.Types (DB (..))
import           Seal.DB.MerklePatricia.Utils
import           Universum (MonadFail)
import qualified Data.ByteString.Char8 as B
import qualified Data.ByteString.Lazy as BSL

import qualified Pos.Util.Modifier as MM
import Data.Text.Encoding
import qualified Data.Text as T


--MM.Modifyer 保存某张表的修改  
data Tbl k = Tbl {
  _tbl :: MM.MapModifier k B.ByteString,
  _tableStateRoot :: StateRoot
  } deriving (Show)
makeLenses ''Tbl

newtype Tables k = Tables {
  _tbls :: MM.MapModifier (Table k) (Tbl k)
  } deriving (Show,Semigroup,Monoid)
makeLenses ''Tables

-- 多余？
data Db = Db {
  _dataTables :: !(Tables DataKey),
  _txTables :: !(Tables TxKey)
  } deriving (Show)
makeLenses ''Db
instance Default Db where def = Db mempty mempty

tblType :: Table k -> Lens' Db (Tables k)
tblType DataTable {} = dataTables
tblType TxTable {} = txTables --todo 是否会有影响

data MPtreeDb = MPtreeDb {
  _temp :: !Db,
  _rootMPDB :: MPDB           
  }
makeLenses ''MPtreeDb

initMPtreeDb :: IO MPtreeDb
initMPtreeDb = do
  db <- openRocksDB "/tmp/contract"
  let rdb' = MPDB {rdb=db,stateRoot=emptyTriePtr}
  initializeBlank rdb'
  return $ MPtreeDb def rdb'

persister :: Persister MPtreeDb
persister = Persister {
  -- 判断mptree中是否有table,没有则创建，同时创建一个modifyer，如果有，则直接创建modifyer
  createTable = \t s -> createTable_ t s,
  beginTx = \_ s -> beginTx_ s
  ,
  -- modifyer 值 提交到mptree 
  commitTx = \s -> commitTx_ s
  ,
  rollbackTx = \s -> rollbackTx_ s
  ,
  -- queryKeys = \t kq s -> (s,) . map fst <$> qry t kq s
  queryKeys = \t kq s -> queryKeys_ t kq s
  ,
  -- query = \t kq s -> fmap (s,) $ qry t kq s >>= mapM (\(k,v) -> (k,) <$> conv v)
  query = \t kq s -> query_ t kq s
  ,
  readValue = \t k s -> readValue_ t k s
  ,
  writeValue = \t wt k v s -> writeValue_ t wt k v s
  ,
  refreshConn = return . (,())
  }

-- 判断mptree中是否有table,没有则创建，同时创建一个modifyer，如果有，则直接创建modifyer
createTable_ :: PactKey k => Table k -> MPtreeDb -> IO (MPtreeDb,())
createTable_ t s = do
  let tables = view (temp . tblType t . tbls) s
  let  baseGetter :: PactKey k => Table k ->  IO (Maybe (Tbl k))
       baseGetter tb = do
        --table k 转换成mpkey
        let mk = tableKey2MPKey tb 
        -- 根据table k 查询对应的table stateroot
        tid <- getKeyVal (_rootMPDB s) mk 
        -- mpVal to  tal k
        return $ mpValToTbl tid
  mtbl <- MM.lookupM baseGetter t tables
  tbs  <- case mtbl of
            Nothing -> return $ MM.insert t (Tbl mempty emptyTriePtr) tables
            Just _ -> throwDbError $ "createTable: already exists: " ++ show t
  let ns = set (temp . tblType t . tbls) tbs s
  return $ (ns,())

--初始化mptree,创建tables树
beginTx_ :: MPtreeDb -> IO (MPtreeDb,())
beginTx_ s = return $ (,()) $ s

commitTx_ :: MPtreeDb -> IO (MPtreeDb,())
commitTx_ s = do
  let tables = _tbls . _dataTables . _temp $ s
  let    ls  = MM.insertions tables
  root <- setMptreeTables ls (_rootMPDB s)
  -- 更新最外层StateRoot
  let ss = set rootMPDB root s
  -- 清空内存DB
  let ns = set temp def ss
  return $ (ns,())

-- reset db to def
rollbackTx_ :: MPtreeDb -> IO (MPtreeDb,())
rollbackTx_ s = 
  return $ (MPtreeDb def (_rootMPDB s),()) 

--先从modifyer中读取，没有，则从mptree中读取
readValue_ :: (PactKey k, PactValue v) => Table k -> k -> MPtreeDb -> IO (MPtreeDb,(Maybe v))
readValue_ t k s = do
  --获取tables
  let tables = view (temp . tblType t . tbls) s
  --通过key，获取对应的value
  --mptree-tables中获取具体的table
  let  baseGetter :: PactKey k => Table k ->  IO (Maybe (Tbl k))
       baseGetter tb = do
        --table k 转换成mpkey
        let mk = tableKey2MPKey tb 
        -- 根据table k 查询对应的table stateroot
        tid <- getKeyVal (_rootMPDB s) mk 
        -- mpVal to  tal k
        return $ mpValToTbl tid
  mtbl <- MM.lookupM baseGetter t tables
  mpv <- case mtbl of 
          --从mptree中读取 
          Nothing     -> throwDbError $ "readValue: no such table: " ++ show t
          Just table  -> do
            let valueGetter :: PactKey k => k -> IO (Maybe B.ByteString)
                valueGetter key = do
                  --将k转换成mpkey
                  let mpkey = pactKey2MPKey key
                  let tMpdb = _rootMPDB s
                  -- tal k 转换成mpdb
                  let mpdb = MPDB {rdb=(rdb tMpdb),stateRoot=(_tableStateRoot table)}
                  --获取对应value
                  val <- getKeyVal mpdb mpkey
                  return val
            MM.lookupM valueGetter k (_tbl table)
  -- ByteString to PactValue 
  let v = mpValToPv mpv          
  return $ (s, v)

--writeValue MM中没有表，判断mptree中是否有，有的话创建一个MM,再写入MM
writeValue_ :: (PactKey k, PactValue v) => Table k -> WriteType -> k -> v -> MPtreeDb -> IO (MPtreeDb,())
writeValue_ t wt k v s = do
  let tables = view (temp . tblType t . tbls) s
  let  baseGetter :: PactKey k => Table k ->  IO (Maybe (Tbl k))
       baseGetter tb = do
        --table k 转换成mpkey
        let mk = tableKey2MPKey tb 
        -- 根据table k 查询对应的table stateroot
        tid <- getKeyVal (_rootMPDB s) mk 
        -- mpVal to  tal k
        return $ mpValToTbl tid
  mtbl <- MM.lookupM baseGetter t tables
  tb <- case mtbl of 
    --从mptree中读取 
    Nothing     -> throwDbError $ "readValue: no such table: " ++ show t
    Just table  -> do
      let valueGetter :: PactKey k => k -> IO (Maybe B.ByteString)
          valueGetter key = do
            --将k转换成mpkey
            let mpkey = pactKey2MPKey key
            let tMpdb = _rootMPDB s
            -- tal k 转换成mpdb
            let mpdb = MPDB {rdb=(rdb tMpdb),stateRoot=(_tableStateRoot table)}
            --获取对应value
            val <- getKeyVal mpdb mpkey
            return val
      mpv <- MM.lookupM valueGetter k (_tbl table)
      ntbl <- case (mpv,wt) of 
               (Just _,Insert) -> throwDbError $ "Insert: value already at key: " ++ show k
               (Nothing,Update) -> throwDbError $ "Update: no value at key: " ++ show k
               _ -> return $ MM.insert k (asByteString v) (_tbl table)
      return $ set tbl ntbl table
  --更新MM中tables
  let newTables = MM.insert t tb tables
  let ns = set (temp . tblType t . tbls) newTables s
  return $ (ns,())   

queryKeys_ :: Table k -> Maybe (KeyQuery k) -> MPtreeDb -> IO (MPtreeDb,[k])
queryKeys_ _ _ _ = undefined

query_ :: Table k -> Maybe (KeyQuery k) -> MPtreeDb -> IO (MPtreeDb,[(k,v)])
query_ _ _ _ = undefined

mpValToPv :: (PactValue v) => Maybe B.ByteString -> Maybe v
mpValToPv (Just v)  = decode . BSL.fromStrict $ v
mpValToPv Nothing = Nothing

mpValToTbl :: PactKey k => Maybe MPVal -> Maybe (Tbl k)
mpValToTbl = fmap(\p -> Tbl {_tbl=mempty, _tableStateRoot=StateRoot p})

pactKey2MPKey :: PactKey k => k -> MPKey
pactKey2MPKey k = bytesToNibbleString $ toByteString k

tableKey2MPKey :: Table k -> MPKey
tableKey2MPKey (DataTable t) = bytesToNibbleString $ sanitize t
tableKey2MPKey (TxTable t) = bytesToNibbleString $ sanitize t

sanitize :: TableId -> B.ByteString
sanitize (TableId t) = encodeUtf8 t

--将tables插入mptree
setMptreeTables :: [(Table DataKey,Tbl DataKey)] -> MPDB -> IO MPDB
setMptreeTables ((k,v):xs) mpdb = do
  let mpKey = tableKey2MPKey k
  let values = MM.insertions (_tbl v)
  tid <- getMPtreeValue mpdb mpKey
  tsr <- case tid of 
    Nothing -> setMPtreeValues values (MPDB {rdb=(rdb mpdb),stateRoot=emptyTriePtr})
    Just tv -> setMPtreeValues values (mpVal2MPDB (rdb mpdb) tv)
  nst <- setMPtreeValue mpKey (mpdb2MPval tsr) mpdb
  setMptreeTables xs nst
setMptreeTables [] mpdb = return mpdb


-- 将modifyer中的值插入table mptree
setMPtreeValues :: [(DataKey,B.ByteString)] -> MPDB -> IO MPDB
setMPtreeValues ((k,v):xs) mpdb = do
  let mpKey = dataKey2MPKey k
  rs <- setMPtreeValue mpKey v mpdb
  --更新stateroot
  setMPtreeValues xs rs
setMPtreeValues [] mpdb = return mpdb  

--Text -ByteString
dataKey2MPKey :: DataKey -> MPKey
dataKey2MPKey (DataKey k) = bytesToNibbleString $ encodeUtf8 k

asByteString :: ToJSON v => v -> B.ByteString
asByteString = BSL.toStrict . encode

mpdb2MPval :: MPDB -> MPVal
mpdb2MPval (MPDB _ (StateRoot sr)) = sr

mpVal2MPDB :: DB -> MPVal -> MPDB
mpVal2MPDB db pval  = MPDB {rdb=db,stateRoot=(StateRoot pval)}


-- MPtree中获取值
getMPtreeValue ::(MonadIO m,MonadFail m)=> MPDB -> MPKey -> m (Maybe MPVal)
getMPtreeValue mpdb key  = do
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

-- qry :: Table k -> Maybe (KeyQuery k) -> MPtreeDb -> IO [(k,PValue)]
-- qry t kq s = case firstOf (temp . tblType t . tbls . ix t . tbl) s of
--   Nothing -> throwDbError $ "query: no such table: " ++ show t
--   Just m -> return $ filter (compileQuery kq . fst) $ M.toList m
-- qry _ _ _ = undefined

-- {-# INLINE qry #-}

_test :: IO ()
_test = do
  e <- initMPtreeDb
  let p = persister
      dt = DataTable "22222"
      -- tt = TxTable "tx"
      run f = do
        s <- get
        (s',r) <- liftIO (f s)
        put s'
        return r
  (`evalStateT` e) $ do
    run $ beginTx p True
    run $ createTable p dt

    -- run $ createTable p tt
    run $ commitTx p
    run $ beginTx p True
    run $ writeValue p dt Insert "stuff" (T.pack "hello")
    run $ writeValue p dt Insert "tough" (String "goodbye")

    -- run $ writeValue p tt Write 1 (String "txy goodness")
    -- run $ writeValue p tt Insert 2 (String "txalicious")
    run $ commitTx p
    run $ writeValue p dt Insert "stuff1" (String "hello")
    -- run $ writeValue p dt Insert "stuff1" (String "hello")

    -- run $ createTable p dt

    run (readValue p dt "tough") >>= (liftIO . (print :: Maybe Text -> IO ()))
    run (readValue p dt "stuff") >>= (liftIO . (print :: Maybe Value -> IO ()))

    -- run (query p dt (Just (KQKey KEQ "stuff"))) >>=
    --   (liftIO . (print :: [(DataKey,Value)] -> IO ()))
    -- run (queryKeys p dt (Just (KQKey KGTE "stuff"))) >>= liftIO . print
    -- run (query p tt (Just (KQKey KGT 0 `kAnd` KQKey KLT 2))) >>=
    --   (liftIO . (print :: [(TxKey,Value)] -> IO ()))
    -- run $ beginTx p True
    -- run $ writeValue p tt Update 2 (String "txalicious-2!")
    -- run (readValue p tt 2) >>= (liftIO . (print :: Maybe Value -> IO ()))
    -- run $ rollbackTx p
    -- run (readValue p tt 2) >>= (liftIO . (print :: Maybe Value -> IO ()))
