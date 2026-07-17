import csv
import io
import json
from typing import Any, Dict, List


class Page:
    """Simulates a Parquet Page containing actual values or dictionary indices."""

    def __init__(self, values: List[Any], min_val: Any, max_val: Any):
        self.values = values
        self.min = min_val
        self.max = max_val


class ColumnChunk:
    """Represents a Column Chunk containing pages and encoding metadata."""

    def __init__(self, column_name: str, data_type: str):
        self.column_name = column_name
        self.data_type = data_type
        self.encoding = "PLAIN"
        self.dictionary: List[str] = []
        self.pages: List[Page] = []

    def load_data(self, values: List[str], page_size: int = 5):
        """Splits raw column values into pages and handles simple dictionary encoding."""
        # Simple heuristic: If it's a string column with low cardinality, use dictionary encoding
        unique_vals = list(set(values))
        if self.data_type == "STRING" and len(unique_vals) <= len(values) * 0.4:
            self.encoding = "DICTIONARY"
            self.dictionary = sorted(unique_vals)
            # Map values to their dictionary index
            encoded_values = [self.dictionary.index(v) for v in values]
            self._create_pages(encoded_values, page_size)
        else:
            # Otherwise, use plain formatting (cast numbers appropriately)
            typed_values = []
            for v in values:
                if not v or v.lower() == "null":
                    typed_values.append(None)
                elif self.data_type == "INT32":
                    typed_values.append(int(v))
                elif self.data_type == "FLOAT":
                    typed_values.append(float(v))
                else:
                    typed_values.append(v)
            self._create_pages(typed_values, page_size)

    def _create_pages(self, values: List[Any], page_size: int):
        for i in range(0, len(values), page_size):
            page_chunk = values[i : i + page_size]
            valid_vals = [v for v in page_chunk if v is not None]
            min_val = min(valid_vals) if valid_vals else None
            max_val = max(valid_vals) if valid_vals else None
            self.pages.append(Page(page_chunk, min_val, max_val))


class RowGroup:
    """A logical horizontal partition of data containing vertical Column Chunks."""

    def __init__(self, row_count: int):
        self.row_count = row_count
        self.columns: Dict[str, ColumnChunk] = {}


class MockParquetWriter:
    """Generates a plain-text human-readable file structure mimicking Parquet."""

    def __init__(self, schema: Dict[str, str]):
        self.schema = schema  # e.g., {"InvoiceNo": "INT32", "Country": "STRING"}
        self.row_groups: List[RowGroup] = []

    def write(self, csv_data: str, row_group_size: int = 10, page_size: int = 5) -> str:
        f = io.StringIO(csv_data.strip())
        reader = csv.DictReader(f)
        all_rows = list(reader)

        # 1. Chunk dataset into Row Groups
        for i in range(0, len(all_rows), row_group_size):
            group_rows = all_rows[i : i + row_group_size]
            row_group = RowGroup(row_count=len(group_rows))

            # Transpose rows to columns within this group
            for col_name, data_type in self.schema.items():
                col_chunk = ColumnChunk(col_name, data_type)
                raw_col_values = [row[col_name] for row in group_rows]
                col_chunk.load_data(raw_col_values, page_size)
                row_group.columns[col_name] = col_chunk

            self.row_groups.append(row_group)

        # 2. Build the string output sequentially to calculate structural positions
        output = []

        # Header
        output.append("|----------------------- Plain-Text Parquet Concept --------------------|")
        output.append("|                                                                       |")
        output.append("| Header                                                                |")
        output.append("| - Magic Number: PAR1                                                  |")
        output.append("|                                                                       |")
        output.append("|---------------------- Data Section (Row Groups) ----------------------|")
        output.append("|                                                                       |")

        # Data Section
        for idx, rg in enumerate(self.row_groups, 1):
            output.append(f"| Row Group {idx}                                                           |")
            for col_name, chunk in rg.columns.items():
                output.append(f"| - Column Chunk ({col_name})")
                if chunk.encoding == "DICTIONARY":
                    output.append(f"|   - Dictionary Page: {chunk.dictionary}")
                for p_idx, page in enumerate(chunk.pages, 1):
                    output.append(f"|   - Page {p_idx} {page.values}")
            output.append("|                                                                       |")

        output.append("|------------------------- Metadata & Footer ---------------------------|")
        output.append("|                                                                       |")

        # Build Footer Metadata Payload
        metadata = {
            "File Metadata": {
                "Schema": self.schema,
                "Created by": "mock-parquet-writer v1.0",
                "Properties": {"author": "Sanjeet Shukla"},
            },
            "Row Groups": [],
        }

        for idx, rg in enumerate(self.row_groups, 1):
            rg_meta = {"Row Group": idx, "Total Rows": rg.row_count, "Column Chunks": {}}
            for col_name, chunk in rg.columns.items():
                rg_meta["Column Chunks"][col_name] = {
                    "Encoding": chunk.encoding,
                    "Pages": [
                        {"Page": p_idx, "Min": page.min, "Max": page.max}
                        for p_idx, page in enumerate(chunk.pages, 1)
                    ],
                }
            metadata["Row Groups"].append(rg_meta)

        # Serialize metadata into the string representation
        metadata_str = json.dumps(metadata, indent=2)
        for line in metadata_str.splitlines():
            output.append(f"| {line}")

        output.append("|                                                                       |")

        # Calculate exact footer length offset before attaching closing elements
        # Sum length of everything written so far plus formatting chars to mock footer byte tracking
        calculated_footer_length = sum(len(line) for line in output)

        output.append(f"| - Footer Length: {calculated_footer_length} bytes")
        output.append("| - Magic Number: PAR1                                                  |")
        output.append("|-----------------------------------------------------------------------|")

        return "\n".join(output)


# ==========================================
# Execution Loop with Synthesized CSV Data
# ==========================================
if __name__ == "__main__":
    # 1. Synthesize target dataset
    synthesized_csv = """InvoiceNo,StockCode,Description,Quantity,Country
536365,85123A,WHITE HANGING HEART T-LIGHT HOLDER,6,United Kingdom
536366,71053,WHITE METAL LANTERN,6,United Kingdom
536367,84029G,KNITTED UNION FLAG HOT WATER BOTTLE,6,United Kingdom
536368,84406B,CREAM CUPID HEARTS COAT HANGER,8,United States
536369,21730,HOME BUILDING BLOCK WORD,6,United Kingdom
536370,37444C,THE BLUE NIGHTINGALE,4,United Kingdom
536371,22083,WHITE OWL IN NIGHT,8,United Kingdom
536372,84971S,RED UNION FLAG HOT WATER BOTTLE,16,United Kingdom
536373,71270,CREAM CUPID HEARTS COAT,4,United Kingdom
536374,47580,HOME BUILDING BLOCK WORD,7,United States"""

    # 2. Define schema types
    target_schema = {
        "InvoiceNo": "INT32",
        "StockCode": "STRING",
        "Description": "STRING",
        "Quantity": "INT32",
        "Country": "STRING",
    }

    # 3. Initialize and run the writer
    writer = MockParquetWriter(schema=target_schema)
    parquet_layout = writer.write(synthesized_csv, row_group_size=10, page_size=5)

    print(parquet_layout)
