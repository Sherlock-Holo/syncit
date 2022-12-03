CREATE TABLE index_files
(
    filename    TEXT    NOT NULL,
    kind        TEXT    NOT NULL,
    gen         INTEGER NOT NULL,
    update_time INTEGER NOT NULL,
    update_by   TEXT    NOT NULL
);
CREATE INDEX idx_filename ON index_files (filename);
