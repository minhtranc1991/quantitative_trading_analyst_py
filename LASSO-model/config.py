import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

def load_config(config_path="config.env"):
    """Load database configuration from .env file."""
    """Load API and database configuration from .env file."""
    load_dotenv(dotenv_path=config_path)
    api_config = {
        "url": os.getenv("API_URL"),
        "key": os.getenv("API_KEY"),
        "secret": os.getenv("SECRET_KEY"),
    }
    db_config = {
        "host": os.getenv("DB_HOST"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "database": os.getenv("DB_NAME"),
        "port": int(os.getenv("DB_PORT", 3306))
    }
    return api_config, db_config

# --- Database Management ---
def create_database_engine():
    """Create a database engine for MySQL."""
    host = db_config['host']
    port = db_config.get('port', 3306)
    if host == 'localhost':
        host = f"{host}:{port}"
    
    return create_engine(
        f"mysql+pymysql://{db_config['user']}:{db_config['password']}@{host}/{db_config['database']}"
    )

def test_database_connection(engine):
    """Test database connection, create a test table, and log verification."""
    try:
        with engine.connect() as connection:
            # Tạo bảng kiểm tra
            connection.execute(text("""
                CREATE TABLE IF NOT EXISTS test_connection (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    test_message VARCHAR(255),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """))
            print("Test table created successfully.")

            # Thêm dữ liệu vào bảng kiểm tra
            connection.execute(text("""
                INSERT INTO test_connection (test_message)
                VALUES ('Database connection test successful.')
            """))
            print("Inserted test data successfully.")

            # Đọc dữ liệu từ bảng kiểm tra
            result = connection.execute(text("SELECT * FROM test_connection"))
            rows = result.fetchall()
            if rows:
                print("Read test data successfully. Log:")
                for row in rows:
                    print(row)
            else:
                print("No test data found.")

            # Xóa bảng kiểm tra
            connection.execute(text("DROP TABLE test_connection"))
            print("Test table dropped successfully. Database connection verified!")
    except Exception as e:
        print(f"Database connection test failed: {e}")

api_config, db_config = load_config()

#print("API Configuration:", api_config)
#print("Database Configuration:", db_config)
#test_database_connection(create_database_engine())