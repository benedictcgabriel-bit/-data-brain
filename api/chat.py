import logging
import os
import re
from typing import Any, Dict, List, Optional, Tuple
try:
    from neo4j import GraphDatabase, Driver
except ImportError:
    GraphDatabase = None
    Driver = None

logger = logging.getLogger("api.chat")


class GroundedChatEngine:
    def __init__(self):
        self.uri = os.getenv("NEO4J_URI", "bolt://neo4j:7687")
        self.user = os.getenv("NEO4J_USER", "neo4j")
        self.password = os.getenv("NEO4J_PASSWORD", "hackathon_secret_password_2026")
        self.database = os.getenv("NEO4J_DATABASE", "neo4j")
        self._driver: Optional[Driver] = None

    def get_driver(self) -> Optional[Driver]:
        if self._driver is not None:
            return self._driver
        try:
            self._driver = GraphDatabase.driver(
                self.uri,
                auth=(self.user, self.password),
                max_connection_lifetime=300,
            )
            return self._driver
        except Exception as e:
            logger.warning(f"Neo4j driver connection failed: {e}")
            return None

    def check_health(self) -> bool:
        driver = self.get_driver()
        if not driver:
            return False
        try:
            with driver.session(database=self.database) as session:
                result = session.run("RETURN 1 AS ping")
                if hasattr(result, "single"):
                    rec = result.single()
                else:
                    rec = result[0] if result else None
                return rec is not None and rec.get("ping") == 1
        except Exception as e:
            logger.debug(f"Neo4j health probe failed: {e}")
            return False

    def get_row_properties(self) -> List[str]:
        driver = self.get_driver()
        if not driver:
            return []
        try:
            with driver.session(database=self.database) as session:
                result = session.run(
                    "MATCH (r:Row) UNWIND keys(r) AS k "
                    "WITH DISTINCT k WHERE NOT k IN ['dataset_id', 'row_index'] "
                    "RETURN k ORDER BY k"
                )
                props = []
                for record in result:
                    props.append(record["k"] if isinstance(record, dict) else record.get("k"))
                return props
        except Exception as e:
            logger.warning(f"Error fetching properties: {e}")
            return []

    def get_row_count(self) -> int:
        driver = self.get_driver()
        if not driver:
            return 0
        try:
            with driver.session(database=self.database) as session:
                result = session.run("MATCH (r:Row) RETURN count(r) AS cnt")
                if hasattr(result, "single"):
                    rec = result.single()
                else:
                    rec = result[0] if result else None
                return rec["cnt"] if rec else 0
        except Exception:
            return 0

    def execute_cypher(self, cypher: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        driver = self.get_driver()
        if not driver:
            raise ConnectionError("Neo4j is not connected")
        with driver.session(database=self.database) as session:
            result = session.run(cypher, params or {})
            records = []
            for record in result:
                records.append(dict(record))
            return records

    def process_query(self, question: str) -> Dict[str, Any]:
        """
        Translates natural language questions to Cypher deterministically based on
        the dynamic graph schema, executes the Cypher query, and returns:
        { answer, cypher, result, grounded }
        """
        q = question.strip()
        q_lower = q.lower()

        # Check if Neo4j has any data
        total_rows = self.get_row_count()
        if total_rows == 0:
            return {
                "answer": "No data is currently loaded in the graph database. Please upload a CSV dataset first.",
                "cypher": "MATCH (r:Row) RETURN count(r) AS total_rows",
                "result": [{"total_rows": 0}],
                "grounded": False,
            }

        properties = self.get_row_properties()
        if not properties:
            return {
                "answer": "No property columns found in the uploaded dataset.",
                "cypher": "NONE",
                "result": [],
                "grounded": False,
            }

        # Build mapping for case-insensitive matching
        prop_map = {p.lower(): p for p in properties}

        # Rule 0: Specific property / properties of entity lookup (e.g. "give me the name, department and role of employee 4")
        prop_of_match = re.search(r"(?:give me|what is|get|find|show|tell me)?\s*(?:the\s+)?(.+?)\s+(?:of|for)\s+(.+?)(?:\?|$)", q_lower)
        if prop_of_match:
            raw_props = prop_of_match.group(1).strip()
            search_entity = prop_of_match.group(2).strip().rstrip("?.!")

            target_props = []
            is_all_details = bool(re.search(r"\b(all|details|detail|info|information|everything|profile)\b", raw_props))
            if is_all_details:
                target_props = list(properties)
            else:
                raw_parts = re.split(r"[,&]|\band\b", raw_props)
                for part in raw_parts:
                    part_clean = re.sub(r"^(the|a|an)\s+", "", part.strip(), flags=re.IGNORECASE).strip()
                    if not part_clean:
                        continue
                    # Match against prop_map
                    matched_p = None
                    for p_low, p_orig in prop_map.items():
                        if p_low == part_clean or p_low.rstrip("s") == part_clean.rstrip("s"):
                            matched_p = p_orig
                            break
                        if p_low.endswith("_" + part_clean) or p_low.startswith(part_clean + "_"):
                            matched_p = p_orig
                            break
                        if part_clean in p_low:
                            matched_p = p_orig
                            break
                    if matched_p and matched_p not in target_props:
                        target_props.append(matched_p)

            if target_props:
                for p in properties:
                    is_exact = search_entity.isdigit() or len(search_entity) <= 3
                    if is_exact:
                        cypher_find = f"MATCH (r:Row) WHERE toLower(toString(r.{p})) = $term RETURN r LIMIT 5"
                    else:
                        cypher_find = f"MATCH (r:Row) WHERE toLower(toString(r.{p})) CONTAINS $term RETURN r LIMIT 5"

                    res = self.execute_cypher(cypher_find, {"term": search_entity.lower()})
                    if res:
                        row_data = res[0].get("r", {})
                        if isinstance(row_data, dict):
                            label = row_data.get("employee_name") or row_data.get("Name") or row_data.get("employee_id") or f"row {search_entity}"
                            if len(target_props) == 1:
                                tp = target_props[0]
                                val = row_data.get(tp)
                                ans = f"For {label}, the {tp} is '{val}'."
                            else:
                                details = ", ".join([f"{tp} is '{row_data.get(tp)}'" for tp in target_props if tp in row_data])
                                ans = f"For {label}: {details}."

                            returns = ", ".join([f"r.{tp} AS {tp}" for tp in target_props])
                            cypher_disp = f"MATCH (r:Row) WHERE toLower(toString(r.{p})) = '{search_entity.lower()}' RETURN {returns}"
                            rec_data = {tp: row_data.get(tp) for tp in target_props if tp in row_data}
                            return {
                                "answer": ans,
                                "cypher": cypher_disp,
                                "result": [rec_data],
                                "grounded": True,
                            }

        # Rule 1: Entity or total count (e.g. "count employees", "count employee", "how many employees", "total employees", "how many products", "how many total rows")
        count_match = re.search(r"\b(how many|count of|total count of|total count|number of|count|total)\s+(?:total\s+)?([a-zA-Z0-9_\-]+)\b", q_lower)
        if count_match or re.search(r"\b(total rows|row count|count rows|how many rows|how many records|count all)\b", q_lower):
            noun = count_match.group(2).lower() if count_match else "rows"
            singular = noun.rstrip("s")

            # If noun refers to a categorical column (e.g. how many departments), count distinct
            col_target = None
            for p_low, p_orig in prop_map.items():
                if p_low == noun or p_low == singular:
                    col_target = p_orig
                    break

            if col_target and col_target.lower() not in ("employee", "product", "item", "row", "record", "name", "id"):
                cypher = f"MATCH (r:Row) WHERE r.{col_target} IS NOT NULL RETURN count(DISTINCT r.{col_target}) AS distinct_{col_target}"
                res = self.execute_cypher(cypher)
                distinct_cnt = res[0][f"distinct_{col_target}"] if res else 0
                return {
                    "answer": f"There are {distinct_cnt} distinct {col_target}s in the knowledge graph.",
                    "cypher": cypher,
                    "result": res,
                    "grounded": True,
                }

            # Otherwise, count total rows/entities if noun matches common entity names, synonyms, or column substrings
            is_entity = singular in ("employee", "product", "item", "row", "record", "entry", "person", "people", "user", "data", "total") or \
                        any(singular in p.lower() for p in properties) or \
                        noun in ("rows", "records", "items", "entries", "total")

            if is_entity or "how many" in q_lower or q_lower.startswith("count"):
                cypher = "MATCH (r:Row) RETURN count(r) AS total_rows"
                res = self.execute_cypher(cypher)
                count_val = res[0]["total_rows"] if res else 0
                if noun in ("total", "all"):
                    display_noun = "rows"
                elif singular in ("employee", "worker", "user", "product", "item", "row", "record", "entry"):
                    display_noun = singular + "s"
                elif singular == "person":
                    display_noun = "people"
                else:
                    display_noun = noun if noun.endswith("s") else noun + "s"

                return {
                    "answer": f"There are {count_val} {display_noun} in the knowledge graph.",
                    "cypher": f"MATCH (r:Row) RETURN count(r) AS total_{display_noun}",
                    "result": [{f"total_{display_noun}": count_val}],
                    "grounded": True,
                }

        # Rule 2: Aggregation queries (average, max, min, sum) on numeric columns
        agg_match = re.search(r"\b(average|avg|mean|maximum|max|highest|minimum|min|lowest|total|sum)\b", q_lower)
        if agg_match:
            agg_word = agg_match.group(1)
            # Find which property is being asked about
            target_prop = None
            for p_low, p_orig in prop_map.items():
                if p_low in q_lower:
                    target_prop = p_orig
                    break

            if target_prop:
                if agg_word in ("average", "avg", "mean"):
                    cypher = f"MATCH (r:Row) WHERE r.{target_prop} IS NOT NULL RETURN round(avg(toInteger(r.{target_prop})), 2) AS average_{target_prop}"
                    res = self.execute_cypher(cypher)
                    val = res[0][f"average_{target_prop}"] if res else None
                    if val is not None:
                        return {
                            "answer": f"The average {target_prop} is {val}.",
                            "cypher": cypher,
                            "result": res,
                            "grounded": True,
                        }
                elif agg_word in ("maximum", "max", "highest"):
                    cypher = f"MATCH (r:Row) WHERE r.{target_prop} IS NOT NULL RETURN max(toInteger(r.{target_prop})) AS max_{target_prop}"
                    res = self.execute_cypher(cypher)
                    val = res[0][f"max_{target_prop}"] if res else None
                    if val is not None:
                        return {
                            "answer": f"The highest {target_prop} is {val}.",
                            "cypher": cypher,
                            "result": res,
                            "grounded": True,
                        }
                elif agg_word in ("minimum", "min", "lowest"):
                    cypher = f"MATCH (r:Row) WHERE r.{target_prop} IS NOT NULL RETURN min(toInteger(r.{target_prop})) AS min_{target_prop}"
                    res = self.execute_cypher(cypher)
                    val = res[0][f"min_{target_prop}"] if res else None
                    if val is not None:
                        return {
                            "answer": f"The lowest {target_prop} is {val}.",
                            "cypher": cypher,
                            "result": res,
                            "grounded": True,
                        }
                elif agg_word in ("total", "sum"):
                    cypher = f"MATCH (r:Row) WHERE r.{target_prop} IS NOT NULL RETURN sum(toInteger(r.{target_prop})) AS sum_{target_prop}"
                    res = self.execute_cypher(cypher)
                    val = res[0][f"sum_{target_prop}"] if res else None
                    if val is not None:
                        return {
                            "answer": f"The total sum of {target_prop} is {val}.",
                            "cypher": cypher,
                            "result": res,
                            "grounded": True,
                        }

        # Rule 3: "How many [employees/people/rows] are in [Department/Category/City/Location/etc]?"
        # Pattern: how many ... in <value>
        in_match = re.search(r"how many\s+(?:.*?\s+)?in\s+([a-zA-Z0-9_\-\s]+)\??", q_lower)
        if in_match:
            val_to_search = in_match.group(1).strip()
            # Clean trailing question marks or punctuation
            val_to_search = re.sub(r"[?!.]", "", val_to_search).strip()

            # Search across categorical properties for this value
            for prop in properties:
                cypher_check = f"MATCH (r:Row) WHERE toLower(toString(r.{prop})) = $val RETURN count(r) AS count"
                res = self.execute_cypher(cypher_check, {"val": val_to_search})
                if res and res[0]["count"] > 0:
                    cnt = res[0]["count"]
                    cypher_display = f"MATCH (r:Row) WHERE toLower(toString(r.{prop})) = '{val_to_search}' RETURN count(r) AS count"
                    return {
                        "answer": f"There are {cnt} records where {prop} is '{val_to_search}'.",
                        "cypher": cypher_display,
                        "result": res,
                        "grounded": True,
                    }

        # Rule 4: List distinct values of a column (e.g. "List all departments", "Show all cities", "What departments exist?")
        list_match = re.search(r"\b(list|show|get|all|distinct|what)\s+(?:all\s+)?([a-zA-Z0-9_]+)", q_lower)
        if list_match:
            potential_col = list_match.group(2).rstrip("s")  # e.g. departments -> department
            for p_low, p_orig in prop_map.items():
                if potential_col in p_low or p_low in potential_col:
                    cypher = f"MATCH (r:Row) WHERE r.{p_orig} IS NOT NULL RETURN DISTINCT r.{p_orig} AS {p_orig} ORDER BY {p_orig}"
                    res = self.execute_cypher(cypher)
                    if res:
                        vals = [str(r[p_orig]) for r in res]
                        return {
                            "answer": f"Distinct {p_orig} values ({len(vals)} found): {', '.join(vals)}.",
                            "cypher": cypher,
                            "result": res,
                            "grounded": True,
                        }

        # Rule 5: Specific entity lookup (e.g. "What is the salary of Alice?", "Tell me about John", "Details for Bob")
        for prop in properties:
            # Check if user asks about a specific name or entity
            name_match = re.search(r"(?:of|for|about)\s+([a-zA-Z0-9_\-\s]+)\??", q_lower)
            if name_match:
                search_entity = re.sub(r"[?!.]", "", name_match.group(1)).strip()
                # Check if there is a row matching Name or similar identifier
                name_props = [p for p in properties if p.lower() in ("name", "employee", "title", "id", "product", "item")]
                for np in name_props:
                    cypher_find = f"MATCH (r:Row) WHERE toLower(toString(r.{np})) CONTAINS $term RETURN r LIMIT 5"
                    res = self.execute_cypher(cypher_find, {"term": search_entity})
                    if res:
                        row_data = res[0]["r"]
                        # Filter out internal keys
                        public_props = {k: v for k, v in row_data.items() if k not in ("dataset_id", "row_index")}
                        details = ", ".join(f"{k}: {v}" for k, v in public_props.items())
                        cypher_disp = f"MATCH (r:Row) WHERE toLower(toString(r.{np})) CONTAINS '{search_entity}' RETURN r LIMIT 5"
                        return {
                            "answer": f"Found record for {search_entity}: {details}.",
                            "cypher": cypher_disp,
                            "result": [public_props],
                            "grounded": True,
                        }

        # Rule 6: Generic filter match (any word in the question matching an exact property value)
        words = [w.strip("?!.,\"'") for w in q.split() if len(w) > 2]
        for w in words:
            w_lower = w.lower()
            if w_lower in ("what", "which", "where", "when", "how", "many", "there", "show", "tell", "have", "with"):
                continue
            for prop in properties:
                cypher_test = f"MATCH (r:Row) WHERE toLower(toString(r.{prop})) = $term RETURN r LIMIT 5"
                res = self.execute_cypher(cypher_test, {"term": w_lower})
                if res:
                    row_data = res[0]["r"]
                    public_props = {k: v for k, v in row_data.items() if k not in ("dataset_id", "row_index")}
                    cypher_disp = f"MATCH (r:Row) WHERE toLower(toString(r.{prop})) = '{w_lower}' RETURN r LIMIT 5"
                    return {
                        "answer": f"Found {len(res)} record(s) matching '{w}' in column '{prop}'.",
                        "cypher": cypher_disp,
                        "result": [public_props],
                        "grounded": True,
                    }

        # Unsupported question: strictly return grounded=false with explicit honest no-data statement
        return {
            "answer": "I do not have data in the knowledge graph to answer this question. The question does not match any properties, entities, or records in the uploaded dataset.",
            "cypher": "NONE",
            "result": [],
            "grounded": False,
        }


chat_engine = GroundedChatEngine()
