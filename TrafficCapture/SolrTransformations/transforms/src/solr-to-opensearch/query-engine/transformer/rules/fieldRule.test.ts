import { describe, it, expect, vi } from 'vitest';
import { fieldRule } from './fieldRule';
import type { FieldNode } from '../../ast/nodes';

/** Stub transformChild — not used by fieldRule but required by signature. */
const stubTransformChild = () => new Map();

describe('fieldRule', () => {
  it.each([
    { field: 'title', value: 'java', desc: 'simple field:value' },
    { field: 'author', value: 'smith', desc: 'simple field name' },
    { field: '_text_', value: 'search', desc: 'underscore field name' },
    { field: 'metadata.author', value: 'doe', desc: 'dotted field name' },
    { field: 'title', value: 'foo-bar_baz.v1@test#123', desc: 'multiple special chars' },
  ])('transforms $desc to match query', ({ field, value }) => {
    const node: FieldNode = { type: 'field', field, value };

    const result = fieldRule(node, stubTransformChild);

    // Uses expanded form: {"match": {"field": {"query": "value"}}}
    expect(result).toEqual(
      new Map([['match', new Map([[field, new Map([['query', value]])]])]]),
    );
  });

  it('transforms existence search field:* to exists query', () => {
    const node: FieldNode = {
      type: 'field',
      field: 'title',
      value: '*',
    };

    const result = fieldRule(node, stubTransformChild);

    expect(result).toEqual(
      new Map([['exists', new Map([['field', 'title']])]]),
    );
  });

  it.each([
    { value: 'te?t', desc: 'single char wildcard (?)' },
    { value: 'tes*ing', desc: '* in middle' },
    { value: 'test*', desc: 'trailing *' },
    { value: '*a', desc: 'leading *' },
    { value: '**', desc: 'double *' },
  ])('throws for wildcard pattern: $desc ($value)', ({ value }) => {
    const node: FieldNode = { type: 'field', field: 'title', value };

    expect(() => fieldRule(node, stubTransformChild)).toThrow(
      `[fieldRule] Wildcard queries aren't supported yet. Query: title:${value}`,
    );
  });

  it('throws for fuzzy search pattern', () => {
    const node: FieldNode = {
      type: 'field',
      field: 'title',
      value: 'roam~',
    };

    expect(() => fieldRule(node, stubTransformChild)).toThrow(
      "[fieldRule] Fuzzy queries aren't supported yet. Query: title:roam~",
    );
  });

  it('throws for fuzzy search with distance', () => {
    const node: FieldNode = {
      type: 'field',
      field: 'title',
      value: 'roam~1',
    };

    expect(() => fieldRule(node, stubTransformChild)).toThrow(
      "[fieldRule] Fuzzy queries aren't supported yet. Query: title:roam~1",
    );
  });

  it.each([
    { value: 'roam~', desc: 'bare tilde' },
    { value: 'roam~1', desc: 'tilde with 1' },
  ])('throws for fuzzy pattern: $desc ($value)', ({ value }) => {
    const node: FieldNode = { type: 'field', field: 'title', value };

    expect(() => fieldRule(node, stubTransformChild)).toThrow(
      `[fieldRule] Fuzzy queries aren't supported yet. Query: title:${value}`,
    );
  });

  it.each([
    { value: 'test~ing', desc: 'tilde in middle' },
    { value: '~test', desc: 'tilde at start' },
    { value: 'a~b~c', desc: 'multiple tildes' },
  ])('does not treat as fuzzy: $desc ($value)', ({ value }) => {
    const node: FieldNode = { type: 'field', field: 'title', value };

    // These should not throw fuzzy error (may throw wildcard or succeed)
    expect(() => fieldRule(node, stubTransformChild)).not.toThrow(/Fuzzy/);
  });

  it('never recurses into children because field is a leaf node', () => {
    const node: FieldNode = { type: 'field', field: 'title', value: 'java' };
    const spy = vi.fn();

    fieldRule(node, spy);

    expect(spy).not.toHaveBeenCalled();
  });

  // --- Field metadata (fieldMappings) ---

  it('uses term query for keyword fields when fieldMappings provided', () => {
    const node: FieldNode = { type: 'field', field: 'category', value: 'electronics' };
    const mappings = new Map([['category', 'keyword']]);

    const result = fieldRule(node, stubTransformChild, mappings);

    expect(result).toEqual(
      new Map([['term', new Map([['category', new Map([['value', 'electronics']])]])]]),
    );
  });

  it('uses match query for text fields when fieldMappings provided', () => {
    const node: FieldNode = { type: 'field', field: 'title', value: 'java' };
    const mappings = new Map([['title', 'text']]);

    const result = fieldRule(node, stubTransformChild, mappings);

    expect(result).toEqual(
      new Map([['match', new Map([['title', new Map([['query', 'java']])]])]]),
    );
  });

  it('uses term query for integer fields', () => {
    const node: FieldNode = { type: 'field', field: 'price', value: '100' };
    const mappings = new Map([['price', 'integer']]);

    const result = fieldRule(node, stubTransformChild, mappings);

    expect(result).toEqual(
      new Map([['term', new Map([['price', new Map([['value', '100']])]])]]),
    );
  });

  it('uses term query for date fields', () => {
    const node: FieldNode = { type: 'field', field: 'created', value: '2025-01-01' };
    const mappings = new Map([['created', 'date']]);

    const result = fieldRule(node, stubTransformChild, mappings);

    expect(result).toEqual(
      new Map([['term', new Map([['created', new Map([['value', '2025-01-01']])]])]]),
    );
  });

  it('falls back to match query when field not in mappings', () => {
    const node: FieldNode = { type: 'field', field: 'unknown_field', value: 'java' };
    const mappings = new Map([['title', 'text']]);

    const result = fieldRule(node, stubTransformChild, mappings);

    expect(result).toEqual(
      new Map([['match', new Map([['unknown_field', new Map([['query', 'java']])]])]]),
    );
  });

  it('falls back to match query when fieldMappings is undefined', () => {
    const node: FieldNode = { type: 'field', field: 'title', value: 'java' };

    const result = fieldRule(node, stubTransformChild, undefined);

    expect(result).toEqual(
      new Map([['match', new Map([['title', new Map([['query', 'java']])]])]]),
    );
  });

  it('term query output uses expanded form for boost compatibility', () => {
    const node: FieldNode = { type: 'field', field: 'category', value: 'electronics' };
    const mappings = new Map([['category', 'keyword']]);
    const result = fieldRule(node, stubTransformChild, mappings);

    const termMap = result.get('term') as Map<string, any>;
    const fieldParams = termMap.get('category');
    expect(fieldParams).toBeInstanceOf(Map);
  });
});
