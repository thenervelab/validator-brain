"""SQL query loader utility."""

import re
from pathlib import Path
from typing import Dict

# Query cache
_QUERIES: Dict[str, str] = {}


def load_query(name: str) -> str:
    """
    Load an SQL query by name.

    The query should be defined in a .sql file in the queries directory,
    and marked with a comment like: -- name: query_name

    Args:
        name: Name of the query to load

    Returns:
        The SQL query string

    Raises:
        KeyError: If query with specified name is not found
    """
    # Return from cache if already loaded
    if name in _QUERIES:
        return _QUERIES[name]

    # Query not in cache, load from files
    queries_dir = Path(__file__).parent / "queries"

    # Pattern to match query definitions
    pattern = re.compile(r"--\s*name:\s*(.+?)$", re.MULTILINE)

    # Go through all .sql files in the queries directory
    for sql_file in queries_dir.glob("*.sql"):
        with open(sql_file) as f:
            content = f.read()

        # Find all query definitions in the file
        matches = pattern.finditer(content)

        # Extract each query
        for match in matches:
            query_name = match.group(1).strip()
            start_pos = match.end() + 1  # +1 to skip the newline

            # Find the end of the query (next query or end of file)
            next_match = pattern.search(content, start_pos)
            if next_match:
                query_content = content[start_pos : next_match.start()].strip()
            else:
                query_content = content[start_pos:].strip()

            # Store in cache
            _QUERIES[query_name] = query_content

    # Check if the requested query was found
    if name not in _QUERIES:
        raise KeyError(f"Query '{name}' not found in any SQL file")

    return _QUERIES[name]
