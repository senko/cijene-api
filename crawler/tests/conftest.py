import pytest
import docker # type: ignore
import time
import psycopg2
from psycopg2 import extras
import os
import shutil
import tempfile
import hashlib

# Configuration for the test database container
POSTGRES_IMAGE = "postgres:15-alpine"
POSTGRES_USER = "testuser"
POSTGRES_PASSWORD = "testpassword"
POSTGRES_DB = "testdb"

# Sample data directory
SAMPLE_DATA_ROOT = os.path.join(os.path.dirname(__file__), "sample_data")


def get_file_hash_util(file_path):
    """Utility function to compute SHA256 hash of a file, identical to the one in import_data.py"""
    sha256_hash = hashlib.sha256()
    try:
        with open(file_path, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()
    except FileNotFoundError:
        return None

@pytest.fixture(scope="session")
def postgresql_container():
    """
    Starts a PostgreSQL Docker container for the test session.
    Yields connection parameters (host, port, user, pass, dbname).
    Stops and removes the container on teardown.
    """
    client = docker.from_env()
    container_name = "pytest_postgres_importer"

    # Remove existing container if it's there from a previous failed run
    try:
        existing_container = client.containers.get(container_name)
        if existing_container:
            existing_container.remove(force=True)
            print(f"Removed existing container: {container_name}")
    except docker.errors.NotFound:
        pass # Container doesn't exist, good.
    except docker.errors.APIError as e:
        print(f"Error checking for existing container {container_name}: {e}")


    print(f"Starting PostgreSQL container '{container_name}'...")
    container = client.containers.run(
        POSTGRES_IMAGE,
        name=container_name,
        environment={
            "POSTGRES_USER": POSTGRES_USER,
            "POSTGRES_PASSWORD": POSTGRES_PASSWORD,
            "POSTGRES_DB": POSTGRES_DB,
        },
        ports={"5432/tcp": None},  # Assign a random available host port
        detach=True,
        remove=False, # Keep it to inspect if needed, will remove manually
    )

    # Get the randomly assigned host port
    container.reload()
    host_port = container.attrs["NetworkSettings"]["Ports"]["5432/tcp"][0]["HostPort"]
    db_params = {
        "host": "localhost",
        "port": host_port,
        "user": POSTGRES_USER,
        "password": POSTGRES_PASSWORD,
        "dbname": POSTGRES_DB,
    }

    # Wait for PostgreSQL to be ready
    retries = 10
    delay = 3  # seconds
    for i in range(retries):
        try:
            conn = psycopg2.connect(**db_params)
            conn.close()
            print(f"PostgreSQL container '{container_name}' is ready on port {host_port}.")
            break
        except psycopg2.OperationalError as e:
            if "password authentication failed" in str(e): # specific error that indicates service is up but auth not yet fully configured
                time.sleep(delay) # give a bit more time for initdb to finish
                continue
            if i < retries - 1:
                print(f"Waiting for PostgreSQL... attempt {i+1}/{retries}. Error: {e}")
                time.sleep(delay)
            else:
                container.logs()
                pytest.fail(f"PostgreSQL container did not become ready: {e}")
    
    yield db_params

    print(f"Stopping and removing PostgreSQL container '{container_name}'...")
    try:
        container.stop()
        container.remove() 
    except docker.errors.NotFound:
        print(f"Container {container_name} already removed.")
    except docker.errors.APIError as e:
        print(f"Error stopping/removing container {container_name}: {e}")


@pytest.fixture(scope="function") # Function scope for a fresh DB connection per test
def test_db_connection(postgresql_container):
    """
    Establishes a psycopg2 connection to the test PostgreSQL database.
    Yields the connection and closes it on teardown.
    """
    conn = None
    try:
        conn = psycopg2.connect(**postgresql_container)
        conn.autocommit = False # Ensure tests can rollback if needed, or manage transactions explicitly
        yield conn
    finally:
        if conn:
            conn.close()


@pytest.fixture(scope="function")
def importer_test_data_dir():
    """
    Creates a temporary directory for CSV data.
    Copies sample data from importer/tests/sample_data/test_chain_1/
    into this temporary directory under a 'test_chain_1' subdirectory.
    Yields the path to this temporary data directory.
    Removes the temporary directory on teardown.
    """
    temp_dir = tempfile.mkdtemp(prefix="importer_test_data_")
    
    # Create the 'test_chain_1' subdirectory within the temp_dir
    chain_data_source_path = os.path.join(SAMPLE_DATA_ROOT, "test_chain_1")
    chain_data_dest_path = os.path.join(temp_dir, "test_chain_1")
    
    if os.path.exists(chain_data_source_path):
        shutil.copytree(chain_data_source_path, chain_data_dest_path)
    else:
        # Create empty dir if source does not exist, so tests can still run and potentially fail
        # on missing files if that's what's being tested
        os.makedirs(chain_data_dest_path, exist_ok=True)
        print(f"Warning: Sample data source path {chain_data_source_path} not found. Created empty {chain_data_dest_path}")

    print(f"Created temporary data directory for importer: {temp_dir}")
    yield temp_dir

    print(f"Removing temporary data directory: {temp_dir}")
    shutil.rmtree(temp_dir)

# Helper to run the importer's main function
# This allows tests to easily call the importer script's logic
def run_importer_main(monkeypatch, db_params, data_dir_path):
    """
    Sets necessary environment variables and calls db_importer.main().
    """
    # Import the target script functions/module
    from crawler.cli import db_importer 

    # Set environment variables that db_importer.connect_db() will use
    monkeypatch.setenv("DB_HOST", db_params["host"])
    monkeypatch.setenv("DB_PORT", str(db_params["port"]))
    monkeypatch.setenv("POSTGRES_USER", db_params["user"]) # Corrected env var name
    monkeypatch.setenv("POSTGRES_PASSWORD", db_params["password"]) # Corrected env var name
    monkeypatch.setenv("POSTGRES_DB", db_params["dbname"]) # Corrected env var name
    
    # DATA_DIR is used by db_importer.import_data, which is called by db_importer.main()
    # The db_importer.main() function will parse --data-path from args, 
    # so we don't strictly need to set DATA_DIR env var if we mock/pass args to main.
    # However, db_importer.py itself does have a module-level DATA_DIR = os.getenv("DATA_DIR", "/data")
    # For consistency IF other functions in db_importer directly used that module-level DATA_DIR,
    # setting it here would be safer. But main() will override it via args.
    # For this setup, we are calling main(), which expects --data-path.
    # So, instead of setting DATA_DIR env var, we need to mock sys.argv for main()
    
    import sys
    original_argv = sys.argv
    sys.argv = ["db_importer.py", "--data-path", data_dir_path] # Mock command line arguments

    # Reload the db_importer module to ensure it picks up monkeypatched os.getenv calls
    # if they are used at module level (which they are for DB_HOST, etc.)
    if 'crawler.cli.db_importer' in sys.modules:
        del sys.modules['crawler.cli.db_importer']
    from crawler.cli import db_importer
    
    # Re-apply module-level vars if db_importer was structured to set them at import time
    # (db_importer.py sets them based on os.getenv at module level, so reload is key)

    # Configure logging for tests to see importer output if needed
    # db_importer.py's main function configures logging based on LOG_LEVEL env var.
    monkeypatch.setenv("LOG_LEVEL", "DEBUG") # Ensure test output is verbose enough

    try:
        db_importer.main() # main() will parse its own args including --data-path
    except SystemExit as e:
        # The script calls sys.exit(1) on DB connection failure or arg parse error.
        if e.code != 0: 
             pytest.fail(f"db_importer.py exited with code {e.code}")
    except Exception as e:
        pytest.fail(f"db_importer.py failed with an unexpected exception: {e}")
    finally:
        sys.argv = original_argv # Restore original sys.argv
```
