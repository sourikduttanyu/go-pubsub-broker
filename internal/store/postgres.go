package store

import (
	"context"
	"encoding/json"

	"github.com/jackc/pgx/v5/pgxpool"
)

const schema = `
CREATE TABLE IF NOT EXISTS broker_messages (
    id           TEXT PRIMARY KEY,
    topic        TEXT NOT NULL,
    data         BYTEA,
    attributes   JSONB,
    published_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS broker_messages_topic_time
    ON broker_messages (topic, published_at DESC);
`

// Postgres persists messages in a PostgreSQL table.
// Connect via NewPostgres; the schema is auto-created on first use.
type Postgres struct {
	pool *pgxpool.Pool
}

// NewPostgres opens a connection pool and creates the schema if needed.
// dsn is a standard PostgreSQL connection string, e.g.:
//
//	postgres://user:pass@localhost:5432/dbname
func NewPostgres(ctx context.Context, dsn string) (*Postgres, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	if _, err := pool.Exec(ctx, schema); err != nil {
		pool.Close()
		return nil, err
	}
	return &Postgres{pool: pool}, nil
}

func (p *Postgres) AppendMessage(msg Message) error {
	attrs, _ := json.Marshal(msg.Attributes)
	_, err := p.pool.Exec(context.Background(),
		`INSERT INTO broker_messages (id, topic, data, attributes, published_at)
		 VALUES ($1, $2, $3, $4, $5)
		 ON CONFLICT (id) DO NOTHING`,
		msg.ID, msg.Topic, msg.Data, attrs, msg.PublishedAt,
	)
	return err
}

func (p *Postgres) LoadMessages(topic string, limit int) ([]Message, error) {
	rows, err := p.pool.Query(context.Background(),
		`SELECT id, topic, data, attributes, published_at
		 FROM broker_messages
		 WHERE topic = $1
		 ORDER BY published_at DESC
		 LIMIT $2`,
		topic, limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var msgs []Message
	for rows.Next() {
		var m Message
		var attrs []byte
		if err := rows.Scan(&m.ID, &m.Topic, &m.Data, &attrs, &m.PublishedAt); err != nil {
			return nil, err
		}
		_ = json.Unmarshal(attrs, &m.Attributes)
		msgs = append(msgs, m)
	}
	if msgs == nil {
		msgs = []Message{}
	}
	return msgs, rows.Err()
}

func (p *Postgres) Close() error {
	p.pool.Close()
	return nil
}
