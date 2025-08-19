from typing import Dict, List, Optional

from carrottransform.tools.mapping_types import ConceptMapping


def generate_combinations(
    value_mapping: Optional[Dict[str, List[int]]],
) -> List[Dict[str, int]]:
    """
    Generate all concept combinations for multiple concept IDs
    NOTE: this logic can handle un-even number of concept IDs across fields, even though this scenario needs more investigation.
    For now, the len of dest_fields should be equal

    For example, if value_mapping is:
    {
        "observation_concept_id": [35827395, 35825531],
        "observation_source_concept_id": [35827395, 35825531]
    }

    This returns:
    [
        {"observation_concept_id": 35827395, "observation_source_concept_id": 35827395},
        {"observation_concept_id": 35825531, "observation_source_concept_id": 35825531}
    ]
    """
    if not value_mapping:
        return []

    # Find the maximum number of concept IDs across all fields
    max_concepts = max(
        len(concept_ids) for concept_ids in value_mapping.values() if concept_ids
    )

    combinations = []
    for i in range(max_concepts):
        combo = {}
        for dest_field, concept_ids in value_mapping.items():
            if concept_ids:
                # Use the concept at index i, or the last one if not enough concepts
                concept_index = min(i, len(concept_ids) - 1)
                combo[dest_field] = concept_ids[concept_index]
        combinations.append(combo)

    return combinations


def get_value_mapping(
    concept_mapping: ConceptMapping, source_value: str
) -> Optional[Dict[str, List[int]]]:
    """
    Get value mapping for a source value, handling wildcards

    Priority:
    1. Exact match for source value
    2. Wildcard match (*) - maps all values to same concept
    3. None
    """
    if source_value in concept_mapping.value_mappings:
        return concept_mapping.value_mappings[source_value]
    elif "*" in concept_mapping.value_mappings:
        return concept_mapping.value_mappings["*"]
    return None
