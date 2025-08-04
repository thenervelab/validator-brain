-- migrate:up
ALTER TABLE registration ADD COLUMN node_hierarchy VARCHAR(10) NOT NULL DEFAULT 'main';

-- migrate:down
ALTER TABLE registration DROP COLUMN node_hierarchy;

