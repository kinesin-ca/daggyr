use super::Result;
use deadpool_postgres::{Client, Manager, ManagerConfig, Pool, RecyclingMethod};
pub use serde::{Deserialize, Serialize};
use std::str::FromStr;
use tokio_postgres::NoTls;

use crate::migrations::{Migration, MIGRATIONS};

use crate::structs::{Parameters, RunID, RunTags};

/*
#[derive(Clone, Debug)]
pub struct Migration {
    up: String,
    down: String,
    path: String,
}

pub struct MigrationsDir {
    pub root: String,
}

impl MigrationsDir {
    pub fn load(&self) -> Result<Vec<Migration>> {
        let mut migrations = Vec::new();
        // read_dir returns the handles in random order, but we need
        // to apply them in lexicographical order
        let mut sources: Vec<String> = std::fs::read_dir(&self.root)?
            .map(|x| x.unwrap().path())
            .filter(|x| x.is_dir())
            .map(|x| x.to_str().unwrap().to_string())
            .collect();

        sources.sort();

        for source in sources {
            let mut base = std::path::PathBuf::from(&source);
            base.push("up.sql");
            let up = String::from(base.to_string_lossy());
            base.pop();
            base.push("down.sql");
            let down = String::from(base.to_string_lossy());
            let migration = Migration {
                up: std::fs::read_to_string(&up)?,
                down: std::fs::read_to_string(&down)?,
                path: source,
            };
            migrations.push(migration);
        }
        Ok(migrations)
    }
}
*/

pub struct Storage {
    pool: Pool,
}

impl Storage {
    pub async fn new(url: &str, max_connections: Option<usize>) -> Self {
        let tokio_config = tokio_postgres::Config::from_str(url).unwrap();
        let mgr_config = ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        };
        let manager = Manager::from_config(tokio_config, NoTls, mgr_config);
        let pool = Pool::builder(manager)
            .max_size(max_connections.unwrap_or(16))
            .build()
            .expect("Unable to build DB pool");
        // Attempt to connect before returning
        let storage = Storage { pool };
        storage.migrate().await.expect("Failed migrations");
        storage
    }

    async fn get_client(&self) -> Client {
        self.pool.get().await.expect("Unable to create client")
    }

    async fn get_last_migration_id(&self, client: &Client) -> Result<usize> {
        let mut last_applied_migration: i32 = 0;
        if let Ok(rows) = client.query("SELECT max(id) from _migrations", &[]).await {
            if !rows.is_empty() && !rows[0].is_empty() {
                last_applied_migration = rows[0].try_get(0).unwrap_or(last_applied_migration);
            }
        } else {
            // Create the table
            client
                    .query("CREATE TABLE _migrations (id SERIAL PRIMARY KEY, path varchar(1024), applied TIMESTAMP default NOW())", &[])
                    .await
                    ?;
        }
        Ok(last_applied_migration as usize)
    }

    pub async fn reset(&self) -> Result<()> {
        let client = self.get_client().await;
        let last_applied_migration = self.get_last_migration_id(&client).await?;
        println!("Last applied migration id is {}", last_applied_migration);
        let mut migrations: Vec<Migration> = MIGRATIONS
            .iter()
            .take(last_applied_migration)
            .cloned()
            .collect();

        migrations.reverse();

        for migration in migrations {
            println!("Unwinding {}", migration.name);
            client.query(migration.down, &[]).await?;
        }

        println!("Clearing migrations table");
        client.query("DELETE FROM _migrations", &[]).await?;

        self.migrate().await?;

        Ok(())
    }

    // migrations
    async fn migrate(&self) -> Result<()> {
        let client = self.get_client().await;
        // Apply outstanding migrations
        let last_applied_migration = self.get_last_migration_id(&client).await?;
        for (i, migration) in MIGRATIONS.iter().enumerate() {
            if i + 1 > last_applied_migration {
                println!("Applying migration {} from {}", i + 1, migration.name);
                client.query(migration.up, &[]).await?;
                client
                    .query(
                        "INSERT INTO _migrations (path) VALUES ($1::TEXT)",
                        &[&migration.name],
                    )
                    .await?;
            }
        }
        Ok(())
    }

    //
    // Auth
    //
    pub async fn auth_user(&self) {}
    pub async fn get_user(&self) {}
    pub async fn get_group(&self) {}

    //
    // Runs
    //

    // Query
    pub async fn create_run(&self, tags: &RunTags, parameters: &Parameters) -> Result<RunID> {
        let run_id;
        let client = self.get_client().await;
        let rows = client
            .query("INSERT INTO runs (buf) VALUES ('a') RETURNING id", &[])
            .await?;
        let rid: i64 = rows[0].try_get(0).unwrap();
        run_id = rid as RunID;

        // Insert parameters
        for (key, vals) in parameters {
            for val in vals {
                client
                    .query("INSERT INTO parameters (run_id, key, value) VALUES ($1::INT, $2::TEXT, $3::TEXT)", &[&rid, &key, &val])
                    .await?;
            }
        }

        // Insert Tags
        for (key, val) in tags.iter() {
            client
                .query("INSERT INTO parameters (run_id, key, value) VALUES ($1::INT, $2::TEXT, $3::TEXT)", &[&rid, &key, &val])
                .await?;
        }

        Ok(run_id)
    }

    /*
    pub async fn add_tasks(&self, run_id: RunID, tasks: &TaskSet) -> Result<()> {
        let client = self.get_client();
        for (task_id, task) in tasks {
            client
                .query("INSERT INTO tasks (run_id, task_id, task_type, is_generator, max_retries) VALUES ($1::INT, $2::TEXT, $3::TEXT, $4::BOOLEAN, $5::INT)", &[&run_id, &task_id, &val])
                .await?;
        }
    }
    */
    pub async fn update_task(&self) {}
    pub async fn update_run_state(&self) {}
    pub async fn update_task_state(&self) {}
    pub async fn add_task_attempt(&self) {}

    // Update
    pub async fn get_runs(&self) {}
    pub async fn get_run(&self) {}
    pub async fn get_run_state(&self) {}
    pub async fn get_run_state_updates(&self) {}
    pub async fn get_task_summary(&self) {}
    pub async fn get_tasks(&self) {}
    pub async fn get_task(&self) {}
    pub async fn get_task_last_attempt(&self) {}
    pub async fn get_task_state(&self) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    fn get_url() -> String {
        let user = users::get_user_by_uid(users::get_current_uid()).unwrap();
        format!(
            "postgres://{}@localhost:5432/daggyr_test",
            user.name().to_string_lossy()
        )
    }

    #[tokio::test]
    async fn test_basic_storage() {
        let url = get_url();
        let storage = Storage::new(&url, None).await;
        storage.reset().await.unwrap();
    }
}
