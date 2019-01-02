{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE DeriveGeneric #-}


module Pact.Server.SealService
 (
    initSealPactService
   ,initSealContract
   ,execSealPactCommand
 ) where

import Pact.Types.Command
import Pact.Types.Logger
import Pact.Interpreter
import Pact.Server.Server(initFastLogger,Config(..),usage)
import qualified Data.Yaml as Y
import Control.Exception
import Data.Default





--mptreedb的路径 --初始化数据库 -- 初始化内置合约
--filepath是否需要读配置文件?
--启动的时候初始化？
initSealPactService :: FilePath -> IO ()
initSealPactService path = do
    Config {..} <- Y.decodeFileEither path >>= \case
      Left e -> do
        putStrLn usage
        throwIO (userError ("Error loading config file: " ++ show e))
      Right v -> return v
    debugFn <- if _verbose then initFastLogger else return (return . const ())
    let loggers = initLoggers debugFn doLog def
        logger  = newLogger loggers "PactService"
        klog s  = logLog logger "INIT" s
    
    p <- mkMPtreeEnv loggers path
    klog "Creating Pact Schema"
    initSchema p
    --  logger = newLogger loggers "PactService"
    





-- 初始化内置合约
initSealContract :: IO ()
initSealContract = undefined

-- 调用合约
execSealPactCommand :: IO CommandResult
execSealPactCommand = undefined