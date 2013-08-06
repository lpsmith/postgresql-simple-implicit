{-# LANGUAGE GeneralizedNewtypeDeriving #-}

-----------------------------------------------------------------------------
-- |
-- Module      :  Database.PostgreSQL.Simple.Implicit
-- Copyright   :  (c) 2013 Leon P Smith
-- License     :  BSD3
--
-- Maintainer  :  leon@melding-monads.com
--
-----------------------------------------------------------------------------

module Database.PostgreSQL.Simple.Implicit where

import Control.Applicative
import Control.Monad.Trans.Reader
import Control.Monad.IO.Class
import Control.Monad.CatchIO
import Data.Int
import Data.Time
import qualified Data.Pool as P
import qualified Database.PostgreSQL.Simple as DB
import qualified Database.PostgreSQL.Simple.Transaction as DB
import qualified Data.ByteString as B


newtype Postgres m a = Postgres (ReaderT ImplicitConnection m a)
  deriving (Functor, Applicative, Monad, MonadIO, MonadCatchIO)


data ImplicitConnection
   = Pool !(P.Pool DB.Connection)
   | Conn !(DB.Connection)

data Config = Config {
      connString     :: !B.ByteString,
      stripeCount    :: !Int,
      keepalive      :: !NominalDiffTime,
      maxConnections :: !Int
    }

defaultConfig :: Config
defaultConfig = Config {
                  connString = B.empty,
                  stripeCount = 1,
                  keepalive = 60,
                  maxConnections = 12
                }

createPostgreSQLPool :: Config -> IO (P.Pool DB.Connection)
createPostgreSQLPool config = do
  P.createPool (DB.connectPostgreSQL (connString config)) DB.close
               (stripeCount config) (keepalive config) (maxConnections config)

runWithPool :: (Monad m) => P.Pool DB.Connection -> Postgres m a -> m a
runWithPool pool (Postgres action) = do
  runReaderT action (Pool pool)

query :: (DB.ToRow q, DB.FromRow r, MonadCatchIO m)
      => DB.Query -> q -> Postgres m [r]
query q ps = liftPostgres (\conn -> DB.query conn q ps)

execute :: (DB.ToRow q, MonadCatchIO m)
        => DB.Query -> q -> Postgres m Int64
execute q ps = liftPostgres (\conn -> DB.execute conn q ps)


withPG :: MonadCatchIO m => Postgres m a -> Postgres m a
withPG (Postgres action) = Postgres $ do
    st <- ask
    case st of 
      Pool p -> P.withResource p $ \c -> do
                  local (const (Conn c)) action
      Conn c -> action

liftPostgres :: MonadCatchIO m => (DB.Connection -> IO a) -> Postgres m a
liftPostgres f = Postgres $ do
    st <- ask
    case st of
      Pool p -> P.withResource p $ \c -> liftIO (f c)
      Conn c -> liftIO (f c)

withTransaction :: MonadCatchIO m => Postgres m a -> Postgres m a
withTransaction = withTransactionMode DB.defaultTransactionMode

withTransactionMode :: MonadCatchIO m
                    => DB.TransactionMode -> Postgres m a -> Postgres m a
withTransactionMode mode (Postgres action) = withPG (Postgres action')
  where
    action' = block $ do
      (Conn c) <- ask
      liftIO $ DB.beginMode mode c
      r <- unblock action `onException` liftIO (DB.rollback c)
      liftIO $ DB.commit c
      return r
