CREATE TABLE file_details
(
    filename    TEXT    NOT NULL,
    gen         INTEGER NOT NULL,
    hash_sum    TEXT    NOT NULL,
    block_chain TEXT,
    deleted     BLOB    NOT NULL
);

CREATE INDEX idx_filename_gen ON file_details (filename, gen);
