/**
 * Where catalogs come from: connectors per source kind, and syncing them into storage.
 */

export {
    DATASETS_DIR,
    DEFAULT_CONNECTOR_DEPS,
    fetchRows,
    type ConnectorDeps,
    type RawRow,
} from "./connectors.ts";
export {
    parseCsv,
} from "./csv.ts";
export {
    importRows,
    mapColumns,
    syncCatalog,
    syncDue,
    syncDueCatalogs,
    toItems,
} from "./sync.ts";
