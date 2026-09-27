// A column a pipeline's saved transforms mask or hash, as GET
// /api/v1/pii/masked-columns reports it. Read from the pipeline configuration,
// not from a scan: a pipeline that masks `email` has already found PII there,
// and pii_scan_results never records that.
export interface PIIMaskedColumn {
  pipeline_id: string;
  pipeline_name: string;
  // The rule's table scope; absent when it applies to every table the
  // pipeline moves.
  table?: string;
  // The masked column, or the dotted path for a nested target.
  column: string;
  // The engine's mask_type: hash, redact, partial, ...
  action: string;
}

interface ScannedColumn {
  table_name: string;
  column_name: string;
}

const tableLeaf = (table: string | undefined) => (table ?? "").split(".").pop()!.trim().toLowerCase();
const columnKey = (column: string) => column.trim().toLowerCase();

/**
 * How many masked columns add to the scan findings: a column a scan already
 * reported is not counted twice, and a column two pipelines both mask counts
 * once. A rule with no table scope matches that column in any scanned table.
 * Tables compare by their last name segment, because a rule may be scoped to
 * `users` while the scan reports `public.users`.
 */
export function maskedColumnsNotScanned(scan: ScannedColumn[], masked: PIIMaskedColumn[]): number {
  const scanned = new Set(scan.map((r) => `${tableLeaf(r.table_name)}|${columnKey(r.column_name)}`));
  const scannedColumns = new Set(scan.map((r) => columnKey(r.column_name)));
  const extra = new Set<string>();
  for (const m of masked) {
    const column = columnKey(m.column);
    if (!column) continue;
    const table = tableLeaf(m.table);
    if (table ? scanned.has(`${table}|${column}`) : scannedColumns.has(column)) continue;
    extra.add(`${table || "*"}|${column}`);
  }
  return extra.size;
}
