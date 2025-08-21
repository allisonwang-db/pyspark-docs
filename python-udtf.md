# Python UDTFs

## Unity Catalog Python UDTFs

Unity Catalog Python User-Defined Table Functions (UDTFs) empower SQL by allowing you to **return complete tables instead of scalar values** in a user-defined function written in Python. Unlike traditional UDFs that return single values, UDTFs can generate multiple rows and columns, making them incredibly powerful for:

### 🚀 **Key Capabilities:**
- **Table Generation**: Transform scalar inputs into multiple rows and columns
- **Complex Data Processing**: Handle nested structures, arrays, and complex transformations
- **External API Integration**: Make REST API calls directly from SQL queries  
- **Stateful Operations**: Maintain state across rows for advanced analytics
- **Real-time Enrichment**: Add computed columns through lateral joins

### 💡 **Perfect Use Cases:**
- **Data Expansion**: Custom "explode" functions for complex data structures
- **API Integration**: IP geolocation, address validation, real-time lookups
- **Data Generation**: Create test data, number sequences, date ranges
- **Custom Analytics**: Implement specialized aggregations and transformations
- **Row-level Processing**: Enrich each row with computed values

### 🔧 **How It Works:**
UDTFs are implemented as **Python classes** with an `eval()` method that uses `yield` to produce output rows dynamically. This allows you to write complex logic in Python while seamlessly integrating with SQL workflows.

*Reference: [Databricks UC Python UDTF Documentation](https://docs.databricks.com/aws/en/udf/udtf-unity-catalog)*

---

### 1. Scalar Input → Scalar Output (Basic UDTF)

**Concept**: Take scalar inputs, produce multiple rows

This is the foundational UC Python UDTF pattern. Unlike regular UDFs that return single values, UDTFs can generate multiple rows from input parameters. The `eval` method uses `yield` to produce each output row as a tuple, making it perfect for data generation and expansion scenarios.

```sql
-- Feature Availability: DBR 17.1+
-- Create UDTF
CREATE OR REPLACE FUNCTION generate_range(start_num INT, end_num INT)
RETURNS TABLE (num INT, squared INT, cubed INT)
LANGUAGE PYTHON
HANDLER 'RangeGenerator'
AS $$
class RangeGenerator:
    def eval(self, start_num: int, end_num: int):
        for num in range(start_num, end_num + 1):
            yield (num, num * num, num * num * num)
$$;

-- Use UDTF
SELECT * FROM generate_range(1, 5);
```

**Tested Output:**
```
num | squared | cubed
----|---------|-------
1   | 1       | 1
2   | 4       | 8
3   | 9       | 27
4   | 16      | 64
5   | 25      | 125
```

---

### 2. Table Arguments (Process entire table)

**Concept**: UDTF processes a table as input, using `__init__`, `eval`, `terminate`

This example introduces table arguments - a major advancement over scalar UDTFs. Instead of processing individual values, the UDTF can process entire tables using the `TABLE(table_name)` syntax. This introduces the stateful processing lifecycle: `__init__` for setup, `eval` for processing each row, and `terminate` for producing final results. This enables aggregation and analytical operations across datasets.

```sql
-- Feature Availability: DBR 17.2+
-- Setup test data
CREATE OR REPLACE TEMP VIEW simple_data AS
SELECT * FROM VALUES (1, 10), (2, 20), (3, 30) AS t(id, value);

-- Create table processing UDTF
CREATE OR REPLACE FUNCTION count_rows(input_table TABLE)
RETURNS TABLE (total_count BIGINT)
LANGUAGE PYTHON
HANDLER 'RowCounter'
AS $$
class RowCounter:
    def __init__(self):
        self.count = 0
        
    def eval(self, row):
        self.count += 1
        
    def terminate(self):
        yield (self.count,)
$$;

-- Use with TABLE() syntax
SELECT * FROM count_rows(TABLE(simple_data));
```

**Tested Output:**
```
total_count
-----------
1
```

**Note**: Without PARTITION BY, only one partition is created.

---

### 3. Table Arguments + Partition By

**Concept**: Control how table data is partitioned for processing

This builds on table arguments by adding partition control. Unlike the previous example where partitioning was implicit, `PARTITION BY` gives explicit control over how data is grouped before being sent to the UDTF. This is crucial for performance and correctness - you can process all data together, or group by specific columns for parallel processing across partitions.

```sql
-- Feature Availability: DBR 17.2+
-- Process all rows in one partition
SELECT * FROM count_rows(TABLE(simple_data) PARTITION BY (1));
-- Result: total_count = 3 (all rows together)

-- Process each row as separate partition  
SELECT * FROM count_rows(TABLE(simple_data) PARTITION BY (id));
-- Result: 3 rows, each with total_count = 1
```

**Tested Outputs:**
- `PARTITION BY (1)`: **Count = 3** (all rows in one partition)
- `PARTITION BY (id)`: **[1, 1, 1]** (each row becomes its own partition)

---

### 4. Stateful Processing (init/eval/terminate)

**Concept**: Accumulate state across rows, produce final results

This example demonstrates the full power of stateful processing, expanding beyond simple counting to complex aggregations. Unlike the previous examples that only counted rows, this UDTF maintains multiple state variables (sum and count) and performs calculations across all rows. This showcases how UDTFs can implement custom analytical functions that would be difficult or impossible with standard SQL aggregates.

```sql
-- Feature Availability: DBR 17.2+
-- Create stateful UDTF that sums values
CREATE OR REPLACE FUNCTION sum_and_count(input_table TABLE)
RETURNS TABLE (total_sum BIGINT, total_count BIGINT)
LANGUAGE PYTHON
HANDLER 'SumCounter'
AS $$
class SumCounter:
    def __init__(self):
        self.sum = 0
        self.count = 0
        
    def eval(self, row):
        self.sum += row['value']
        self.count += 1
        
    def terminate(self):
        yield (self.sum, self.count)
$$;

-- Use with all data in one partition
SELECT * FROM sum_and_count(TABLE(simple_data) PARTITION BY (1));
```

**Tested Output:**
```
total_sum | total_count
----------|------------
60        | 3
```

---

### 5. Lateral Join (Expand table data)

**Concept**: Use UDTF to add computed columns to existing table

This pattern returns to scalar input UDTFs but demonstrates a different usage pattern - lateral joins for row-by-row enrichment. Unlike the table argument examples that process entire tables, this approach applies the UDTF to each row individually using the lateral join syntax. This is perfect for data enrichment scenarios where you need to add computed columns based on existing data, such as IP geolocation, text analysis, or any row-level transformations.

```sql
-- Feature Availability: DBR 17.1+
-- Setup test data
CREATE OR REPLACE TEMP VIEW ip_data AS
SELECT * FROM VALUES 
  (1, '192.168.1.1'),
  (2, '8.8.8.8')
AS t(id, ip_address);

-- Create simple UDTF for IP analysis
CREATE OR REPLACE FUNCTION simple_ip_check(ip STRING)
RETURNS TABLE (is_private BOOLEAN)
LANGUAGE PYTHON
HANDLER 'SimpleIpChecker'
AS $$
class SimpleIpChecker:
    def eval(self, ip: str):
        is_private = ip.startswith('192.168.') or ip.startswith('10.')
        yield (is_private,)
$$;

-- Use lateral join (comma syntax)
SELECT i.id, i.ip_address, l.is_private
FROM ip_data i, LATERAL simple_ip_check(i.ip_address) l;
```

**Tested Output:**
```
id | ip_address  | is_private
---|-------------|----------
1  | 192.168.1.1 | true
2  | 8.8.8.8     | false
```

---
