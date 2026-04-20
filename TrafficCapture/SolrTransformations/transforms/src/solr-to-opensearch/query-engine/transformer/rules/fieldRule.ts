/**
 * Transformation rule for FieldNode → OpenSearch query.
 *
 * Maps Solr's field:value syntax to the appropriate OpenSearch query type:
 *   - field:* (existence) → exists query
 *   - field:value (keyword field) → term query (exact match)
 *   - field:value (text field or unknown) → match query (analyzed)
 *
 * When fieldMappings are provided, the rule uses the field's OpenSearch type
 * to choose between term (keyword/numeric/date) and match (text) queries.
 * Without fieldMappings, defaults to match query for backward compatibility.
 *
 * Unsupported (throws error):
 *   - Wildcards (te?t, tes*) - throws error
 *   - Fuzzy searches (roam~, roam~1) - throws error
 */

import type { ASTNode } from '../../ast/nodes';
import type { TransformRuleFn, FieldMappings } from '../types';

/** Regex to detect wildcard patterns (contains * or ?) */
const WILDCARD_PATTERN = /[*?]/;

/** Regex to detect fuzzy search patterns (term~ or term~N at end) */
const FUZZY_PATTERN = /~\d?$/;

/** OpenSearch field types that should use term query (exact match, not analyzed) */
const KEYWORD_TYPES = new Set(['keyword', 'integer', 'long', 'float', 'double', 'boolean', 'date', 'ip']);

export const fieldRule: TransformRuleFn = (
  node: ASTNode,
  _transformChild,
  fieldMappings?: FieldMappings,
): Map<string, any> => {
  const { field, value } = node;

  // Existence search (field:*) → exists query
  if (value === '*') {
    return new Map([['exists', new Map([['field', field]])]]);
  }

  // Detect unsupported fuzzy patterns
  if (FUZZY_PATTERN.test(value)) {
    const msg = `[fieldRule] Fuzzy queries aren't supported yet. Query: ${field}:${value}`;
    console.error(msg);
    throw new Error(msg);
  }

  // Detect unsupported wildcard patterns
  if (WILDCARD_PATTERN.test(value)) {
    const msg = `[fieldRule] Wildcard queries aren't supported yet. Query: ${field}:${value}`;
    console.error(msg);
    throw new Error(msg);
  }

  // Choose query type based on field metadata
  const fieldType = fieldMappings?.get(field);
  if (fieldType && KEYWORD_TYPES.has(fieldType)) {
    // Keyword/numeric/date fields → term query (exact match, no analysis)
    return new Map([['term', new Map([[field, new Map([['value', value]])]])]]);
  }

  // Default: text fields or unknown → match query (analyzed)
  return new Map([['match', new Map([[field, new Map([['query', value]])]])]]);
};
