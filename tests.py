import unittest
import pandas as pd
import sqlite3
import os
import sys
import tempfile
import shutil
from datetime import datetime

# Add parent directory to path to import the pipeline module
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import the DataPipeline class from your actual implementation
# This assumes your pipeline code is in a file called pipeline.py in the root directory
from pipeline import DataPipeline

class TestDataPipeline(unittest.TestCase):
    """Unit tests for the E-commerce Data Pipeline"""
    
    def setUp(self):
        """Set up test environment before each test"""
        # Create temporary directory for test data
        self.test_data_dir = tempfile.mkdtemp()
        self.test_db_path = os.path.join(self.test_data_dir, 'test_ecommerce.db')
        
        # Create test data files
        self.create_test_data()
        
        # Initialize pipeline with test data
        self.pipeline = DataPipeline(data_dir=self.test_data_dir, db_path=self.test_db_path)
    
    def tearDown(self):
        """Clean up after each test"""
        # Remove temporary directory and all its contents
        shutil.rmtree(self.test_data_dir)
    
    def create_test_data(self):
        """Create sample data files for testing"""
        # Create a small set of test data
        
        # Customers data
        customers_data = [
            {'customer_id': 1, 'name': 'John Doe', 'email': 'john@example.com', 
             'address': '123 Main St', 'phone': '555-1234', 
             'registration_date': '2023-01-15'},
            {'customer_id': 2, 'name': 'Jane Smith', 'email': 'jane@example.com', 
             'address': '456 Oak Ave', 'phone': '555-5678', 
             'registration_date': '2023-02-20'},
            {'customer_id': 3, 'name': 'Bob Brown', 'email': 'bob@example.com', 
             'address': '789 Pine Rd', 'phone': '555-9012', 
             'registration_date': '2023-03-10'}
        ]
        
        # Products data
        products_data = [
            {'product_id': 1, 'name': 'Laptop', 'category': 'Electronics', 
             'price': 1200.00, 'stock': 50},
            {'product_id': 2, 'name': 'Smartphone', 'category': 'Electronics', 
             'price': 800.00, 'stock': 100},
            {'product_id': 3, 'name': 'T-shirt', 'category': 'Clothing', 
             'price': 25.00, 'stock': 200},
            {'product_id': 4, 'name': 'Coffee Maker', 'category': 'Home & Kitchen', 
             'price': 150.00, 'stock': 30}
        ]
        
        # Orders data
        orders_data = [
            {'order_id': 1, 'customer_id': 1, 'order_date': '2023-06-01', 
             'status': 'Completed', 'total': 1200.00},
            {'order_id': 2, 'customer_id': 2, 'order_date': '2023-06-15', 
             'status': 'Shipped', 'total': 825.00},
            {'order_id': 3, 'customer_id': 1, 'order_date': '2023-07-05', 
             'status': 'Completed', 'total': 150.00},
            {'order_id': 4, 'customer_id': 3, 'order_date': '2023-07-10', 
             'status': 'Processing', 'total': 800.00}
        ]
        
        # Order items data
        order_items_data = [
            {'order_id': 1, 'product_id': 1, 'quantity': 1, 'price': 1200.00, 'total': 1200.00},
            {'order_id': 2, 'product_id': 2, 'quantity': 1, 'price': 800.00, 'total': 800.00},
            {'order_id': 2, 'product_id': 3, 'quantity': 1, 'price': 25.00, 'total': 25.00},
            {'order_id': 3, 'product_id': 4, 'quantity': 1, 'price': 150.00, 'total': 150.00},
            {'order_id': 4, 'product_id': 2, 'quantity': 1, 'price': 800.00, 'total': 800.00}
        ]
        
        # Convert to DataFrames
        customers_df = pd.DataFrame(customers_data)
        products_df = pd.DataFrame(products_data)
        orders_df = pd.DataFrame(orders_data)
        order_items_df = pd.DataFrame(order_items_data)
        
        # Save to CSV files in the test directory
        customers_df.to_csv(os.path.join(self.test_data_dir, 'customers.csv'), index=False)
        products_df.to_csv(os.path.join(self.test_data_dir, 'products.csv'), index=False)
        orders_df.to_csv(os.path.join(self.test_data_dir, 'orders.csv'), index=False)
        order_items_df.to_csv(os.path.join(self.test_data_dir, 'order_items.csv'), index=False)
    
    def test_extract_phase(self):
        """Test the extract phase of the pipeline"""
        # Run the extract phase
        result = self.pipeline.extract()
        
        # Assert that extraction was successful
        self.assertTrue(result)
        
        # Check if data was properly loaded
        self.assertEqual(len(self.pipeline.customers), 3)
        self.assertEqual(len(self.pipeline.products), 4)
        self.assertEqual(len(self.pipeline.orders), 4)
        self.assertEqual(len(self.pipeline.order_items), 5)
        
        # Check specific values
        self.assertEqual(self.pipeline.customers.loc[0, 'name'], 'John Doe')
        self.assertEqual(self.pipeline.products.loc[0, 'category'], 'Electronics')
    
    def test_transform_phase(self):
        """Test the transformation phase of the pipeline"""
        # Run extract first, then transform
        self.pipeline.extract()
        result = self.pipeline.transform()
        
        # Assert that transformation was successful
        self.assertTrue(result)
        
        # Check if transformations were applied correctly
        self.assertIn('order_month', self.pipeline.orders.columns)
        self.assertIn('order_year', self.pipeline.orders.columns)
        self.assertIn('lifetime_value', self.pipeline.customers.columns)
        self.assertIn('segment', self.pipeline.customers.columns)
        
        # Check specific transformations
        # John has two orders totaling $1350
        john_lifetime_value = self.pipeline.customers[
            self.pipeline.customers['customer_id'] == 1
        ]['lifetime_value'].values[0]
        self.assertAlmostEqual(john_lifetime_value, 1350.0)
    
    def test_load_phase(self):
        """Test the load phase of the pipeline"""
        # Run extract and transform first, then load
        self.pipeline.extract()
        self.pipeline.transform()
        result = self.pipeline.load()
        
        # Assert that loading was successful
        self.assertTrue(result)
        
        # Check if database was created
        self.assertTrue(os.path.exists(self.test_db_path))
        
        # Connect to the database and verify data
        conn = sqlite3.connect(self.test_db_path)
        cursor = conn.cursor()
        
        # Check if tables exist
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        table_names = [table[0] for table in tables]
        
        self.assertIn('customers', table_names)
        self.assertIn('products', table_names)
        self.assertIn('orders', table_names)
        self.assertIn('order_items', table_names)
        
        # Check if views were created
        cursor.execute("SELECT name FROM sqlite_master WHERE type='view';")
        views = cursor.fetchall()
        view_names = [view[0] for view in views]
        
        self.assertIn('monthly_sales', view_names)
        self.assertIn('product_performance', view_names)
        
        # Check record counts
        cursor.execute("SELECT COUNT(*) FROM customers;")
        self.assertEqual(cursor.fetchone()[0], 3)
        
        cursor.execute("SELECT COUNT(*) FROM products;")
        self.assertEqual(cursor.fetchone()[0], 4)
        
        # Close connection
        conn.close()
    
    def test_run_pipeline(self):
        """Test running the complete pipeline"""
        # Run the complete pipeline
        result = self.pipeline.run_pipeline()
        
        # Assert that the pipeline completed successfully
        self.assertTrue(result)
        
        # Verify database was created with transformed data
        conn = sqlite3.connect(self.test_db_path)
        
        # Query the database to check if transformations are present
        df = pd.read_sql_query("SELECT * FROM customers", conn)
        self.assertIn('segment', df.columns)
        
        # Check the monthly_sales view
        df = pd.read_sql_query("SELECT * FROM monthly_sales", conn)
        self.assertTrue(len(df) > 0)
        
        # Close connection
        conn.close()
    
    def test_pipeline_with_missing_data(self):
        """Test the pipeline's behavior with missing data files"""
        # Remove one of the data files
        os.remove(os.path.join(self.test_data_dir, 'orders.csv'))
        
        # Run the pipeline and check that it fails gracefully
        result = self.pipeline.run_pipeline()
        
        # Pipeline should fail at extraction phase
        self.assertFalse(result)
        
        # Restore the file for cleanup
        self.create_test_data()


if __name__ == '__main__':
    unittest.main()
