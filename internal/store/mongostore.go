package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	defaultMongoDatabase   = "ai-provider"
	defaultMongoAuthColl   = "cliproxy_auth_store"
	defaultMongoConfigColl = "cliproxy_config_store"
	defaultMongoConfigKey  = "config"
)

// MongoStoreConfig captures configuration required to initialize a MongoDB-backed store.
type MongoStoreConfig struct {
	URI        string
	Database   string
	AuthColl   string
	ConfigColl string
	SpoolDir   string
}

// MongoStore persists configuration and authentication metadata using MongoDB as backend
// while mirroring data to a local workspace so existing file-based workflows continue to operate.
type MongoStore struct {
	client     *mongo.Client
	db         *mongo.Database
	authColl   *mongo.Collection
	configColl *mongo.Collection
	spoolRoot  string
	configPath string
	authDir    string
	mu         sync.Mutex
}

// authDocument represents the schema for auth records in MongoDB.
type authDocument struct {
	ID        string    `bson:"_id"`
	Content   bson.Raw  `bson:"content"`
	CreatedAt time.Time `bson:"created_at"`
	UpdatedAt time.Time `bson:"updated_at"`
}

// configDocument represents the schema for config records in MongoDB.
type configDocument struct {
	ID        string    `bson:"_id"`
	Content   string    `bson:"content"`
	CreatedAt time.Time `bson:"created_at"`
	UpdatedAt time.Time `bson:"updated_at"`
}

// NewMongoStore establishes a connection to MongoDB and prepares the local workspace.
func NewMongoStore(ctx context.Context, cfg MongoStoreConfig) (*MongoStore, error) {
	trimmedURI := strings.TrimSpace(cfg.URI)
	if trimmedURI == "" {
		return nil, fmt.Errorf("mongo store: URI is required")
	}
	cfg.URI = trimmedURI

	if cfg.Database == "" {
		cfg.Database = defaultMongoDatabase
	}
	if cfg.AuthColl == "" {
		cfg.AuthColl = defaultMongoAuthColl
	}
	if cfg.ConfigColl == "" {
		cfg.ConfigColl = defaultMongoConfigColl
	}

	spoolRoot := strings.TrimSpace(cfg.SpoolDir)
	if spoolRoot == "" {
		if cwd, err := os.Getwd(); err == nil {
			spoolRoot = filepath.Join(cwd, "mongostore")
		} else {
			spoolRoot = filepath.Join(os.TempDir(), "mongostore")
		}
	}
	absSpool, err := filepath.Abs(spoolRoot)
	if err != nil {
		return nil, fmt.Errorf("mongo store: resolve spool directory: %w", err)
	}
	configDir := filepath.Join(absSpool, "config")
	authDir := filepath.Join(absSpool, "auths")
	if err = os.MkdirAll(configDir, 0o700); err != nil {
		return nil, fmt.Errorf("mongo store: create config directory: %w", err)
	}
	if err = os.MkdirAll(authDir, 0o700); err != nil {
		return nil, fmt.Errorf("mongo store: create auth directory: %w", err)
	}

	clientOpts := options.Client().ApplyURI(cfg.URI)
	client, err := mongo.Connect(ctx, clientOpts)
	if err != nil {
		return nil, fmt.Errorf("mongo store: connect to database: %w", err)
	}
	if err = client.Ping(ctx, nil); err != nil {
		_ = client.Disconnect(ctx)
		return nil, fmt.Errorf("mongo store: ping database: %w", err)
	}

	db := client.Database(cfg.Database)
	store := &MongoStore{
		client:     client,
		db:         db,
		authColl:   db.Collection(cfg.AuthColl),
		configColl: db.Collection(cfg.ConfigColl),
		spoolRoot:  absSpool,
		configPath: filepath.Join(configDir, "config.yaml"),
		authDir:    authDir,
	}
	return store, nil
}

// Close releases the underlying MongoDB connection.
func (s *MongoStore) Close(ctx context.Context) error {
	if s == nil || s.client == nil {
		return nil
	}
	return s.client.Disconnect(ctx)
}

// EnsureCollections creates the required indexes for auth and config collections.
func (s *MongoStore) EnsureCollections(ctx context.Context) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("mongo store: not initialized")
	}

	// Auth collection - _id is automatically indexed as primary key
	// Config collection - _id is automatically indexed as primary key
	// MongoDB handles unique constraint on _id by default

	// Create compound index on auth collection for faster queries
	authIndexModel := mongo.IndexModel{
		Keys:    bson.D{{Key: "updated_at", Value: -1}},
		Options: options.Index().SetBackground(true),
	}
	if _, err := s.authColl.Indexes().CreateOne(ctx, authIndexModel); err != nil {
		log.WithError(err).Warn("mongo store: failed to create updated_at index on auth collection")
	}

	return nil
}

// Bootstrap synchronizes configuration and auth records between MongoDB and the local workspace.
func (s *MongoStore) Bootstrap(ctx context.Context, exampleConfigPath string) error {
	if err := s.EnsureCollections(ctx); err != nil {
		return err
	}
	if err := s.syncConfigFromDatabase(ctx, exampleConfigPath); err != nil {
		return err
	}
	if err := s.syncAuthFromDatabase(ctx); err != nil {
		return err
	}
	return nil
}

// ConfigPath returns the managed configuration file path inside the spool directory.
func (s *MongoStore) ConfigPath() string {
	if s == nil {
		return ""
	}
	return s.configPath
}

// AuthDir returns the local directory containing mirrored auth files.
func (s *MongoStore) AuthDir() string {
	if s == nil {
		return ""
	}
	return s.authDir
}

// WorkDir exposes the root spool directory used for mirroring.
func (s *MongoStore) WorkDir() string {
	if s == nil {
		return ""
	}
	return s.spoolRoot
}

// SetBaseDir implements the optional interface used by authenticators; it is a no-op because
// the MongoDB-backed store controls its own workspace.
func (s *MongoStore) SetBaseDir(string) {}

// Save persists authentication metadata to disk and MongoDB.
func (s *MongoStore) Save(ctx context.Context, auth *cliproxyauth.Auth) (string, error) {
	if auth == nil {
		return "", fmt.Errorf("mongo store: auth is nil")
	}

	path, err := s.resolveAuthPath(auth)
	if err != nil {
		return "", err
	}
	if path == "" {
		return "", fmt.Errorf("mongo store: missing file path attribute for %s", auth.ID)
	}

	if auth.Disabled {
		if _, statErr := os.Stat(path); errors.Is(statErr, fs.ErrNotExist) {
			return "", nil
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if err = os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return "", fmt.Errorf("mongo store: create auth directory: %w", err)
	}

	switch {
	case auth.Storage != nil:
		if err = auth.Storage.SaveTokenToFile(path); err != nil {
			return "", err
		}
	case auth.Metadata != nil:
		raw, errMarshal := json.Marshal(auth.Metadata)
		if errMarshal != nil {
			return "", fmt.Errorf("mongo store: marshal metadata: %w", errMarshal)
		}
		if existing, errRead := os.ReadFile(path); errRead == nil {
			if jsonEqual(existing, raw) {
				return path, nil
			}
		} else if errRead != nil && !errors.Is(errRead, fs.ErrNotExist) {
			return "", fmt.Errorf("mongo store: read existing metadata: %w", errRead)
		}
		tmp := path + ".tmp"
		if errWrite := os.WriteFile(tmp, raw, 0o600); errWrite != nil {
			return "", fmt.Errorf("mongo store: write temp auth file: %w", errWrite)
		}
		if errRename := os.Rename(tmp, path); errRename != nil {
			return "", fmt.Errorf("mongo store: rename auth file: %w", errRename)
		}
	default:
		return "", fmt.Errorf("mongo store: nothing to persist for %s", auth.ID)
	}

	if auth.Attributes == nil {
		auth.Attributes = make(map[string]string)
	}
	auth.Attributes["path"] = path

	if strings.TrimSpace(auth.FileName) == "" {
		auth.FileName = auth.ID
	}

	relID, err := s.relativeAuthID(path)
	if err != nil {
		return "", err
	}
	if err = s.upsertAuthRecord(ctx, relID, path); err != nil {
		return "", err
	}
	return path, nil
}

// List enumerates all auth records stored in MongoDB.
func (s *MongoStore) List(ctx context.Context) ([]*cliproxyauth.Auth, error) {
	cursor, err := s.authColl.Find(ctx, bson.M{}, options.Find().SetSort(bson.D{{Key: "_id", Value: 1}}))
	if err != nil {
		return nil, fmt.Errorf("mongo store: list auth: %w", err)
	}
	defer cursor.Close(ctx)

	auths := make([]*cliproxyauth.Auth, 0, 32)
	for cursor.Next(ctx) {
		var doc authDocument
		if err = cursor.Decode(&doc); err != nil {
			return nil, fmt.Errorf("mongo store: decode auth document: %w", err)
		}
		path, errPath := s.absoluteAuthPath(doc.ID)
		if errPath != nil {
			log.WithError(errPath).Warnf("mongo store: skipping auth %s outside spool", doc.ID)
			continue
		}
		metadata := make(map[string]any)
		if err = bson.Unmarshal(doc.Content, &metadata); err != nil {
			log.WithError(err).Warnf("mongo store: skipping auth %s with invalid bson", doc.ID)
			continue
		}
		provider := strings.TrimSpace(valueAsString(metadata["type"]))
		if provider == "" {
			provider = "unknown"
		}
		attr := map[string]string{"path": path}
		if email := strings.TrimSpace(valueAsString(metadata["email"])); email != "" {
			attr["email"] = email
		}
		auth := &cliproxyauth.Auth{
			ID:               normalizeAuthID(doc.ID),
			Provider:         provider,
			FileName:         normalizeAuthID(doc.ID),
			Label:            labelFor(metadata),
			Status:           cliproxyauth.StatusActive,
			Attributes:       attr,
			Metadata:         metadata,
			CreatedAt:        doc.CreatedAt,
			UpdatedAt:        doc.UpdatedAt,
			LastRefreshedAt:  time.Time{},
			NextRefreshAfter: time.Time{},
		}
		auths = append(auths, auth)
	}
	if err = cursor.Err(); err != nil {
		return nil, fmt.Errorf("mongo store: iterate auth cursor: %w", err)
	}
	return auths, nil
}

// Delete removes an auth file and the corresponding database record.
func (s *MongoStore) Delete(ctx context.Context, id string) error {
	id = strings.TrimSpace(id)
	if id == "" {
		return fmt.Errorf("mongo store: id is empty")
	}
	path, err := s.resolveDeletePath(id)
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if err = os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("mongo store: delete auth file: %w", err)
	}
	relID, err := s.relativeAuthID(path)
	if err != nil {
		return err
	}
	return s.deleteAuthRecord(ctx, relID)
}

// PersistAuthFiles stores the provided auth file changes in MongoDB.
func (s *MongoStore) PersistAuthFiles(ctx context.Context, _ string, paths ...string) error {
	if len(paths) == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, p := range paths {
		trimmed := strings.TrimSpace(p)
		if trimmed == "" {
			continue
		}
		relID, err := s.relativeAuthID(trimmed)
		if err != nil {
			// Attempt to resolve absolute path under authDir.
			abs := trimmed
			if !filepath.IsAbs(abs) {
				abs = filepath.Join(s.authDir, trimmed)
			}
			relID, err = s.relativeAuthID(abs)
			if err != nil {
				log.WithError(err).Warnf("mongo store: ignoring auth path %s", trimmed)
				continue
			}
			trimmed = abs
		}
		if err = s.syncAuthFile(ctx, relID, trimmed); err != nil {
			return err
		}
	}
	return nil
}

// PersistConfig mirrors the local configuration file to MongoDB.
func (s *MongoStore) PersistConfig(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	data, err := os.ReadFile(s.configPath)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return s.deleteConfigRecord(ctx)
		}
		return fmt.Errorf("mongo store: read config file: %w", err)
	}
	return s.persistConfig(ctx, data)
}

// syncConfigFromDatabase writes the database-stored config to disk or seeds the database from template.
func (s *MongoStore) syncConfigFromDatabase(ctx context.Context, exampleConfigPath string) error {
	var doc configDocument
	err := s.configColl.FindOne(ctx, bson.M{"_id": defaultMongoConfigKey}).Decode(&doc)
	switch {
	case errors.Is(err, mongo.ErrNoDocuments):
		if _, errStat := os.Stat(s.configPath); errors.Is(errStat, fs.ErrNotExist) {
			if exampleConfigPath != "" {
				if errCopy := copyConfigTemplate(exampleConfigPath, s.configPath); errCopy != nil {
					return fmt.Errorf("mongo store: copy example config: %w", errCopy)
				}
			} else {
				if errCreate := os.MkdirAll(filepath.Dir(s.configPath), 0o700); errCreate != nil {
					return fmt.Errorf("mongo store: prepare config directory: %w", errCreate)
				}
				if errWrite := os.WriteFile(s.configPath, []byte{}, 0o600); errWrite != nil {
					return fmt.Errorf("mongo store: create empty config: %w", errWrite)
				}
			}
		}
		data, errRead := os.ReadFile(s.configPath)
		if errRead != nil {
			return fmt.Errorf("mongo store: read local config: %w", errRead)
		}
		if errPersist := s.persistConfig(ctx, data); errPersist != nil {
			return errPersist
		}
	case err != nil:
		return fmt.Errorf("mongo store: load config from database: %w", err)
	default:
		if err = os.MkdirAll(filepath.Dir(s.configPath), 0o700); err != nil {
			return fmt.Errorf("mongo store: prepare config directory: %w", err)
		}
		normalized := normalizeLineEndings(doc.Content)
		if err = os.WriteFile(s.configPath, []byte(normalized), 0o600); err != nil {
			return fmt.Errorf("mongo store: write config to spool: %w", err)
		}
	}
	return nil
}

// syncAuthFromDatabase populates the local auth directory from MongoDB data.
func (s *MongoStore) syncAuthFromDatabase(ctx context.Context) error {
	cursor, err := s.authColl.Find(ctx, bson.M{})
	if err != nil {
		return fmt.Errorf("mongo store: load auth from database: %w", err)
	}
	defer cursor.Close(ctx)

	if err = os.RemoveAll(s.authDir); err != nil {
		return fmt.Errorf("mongo store: reset auth directory: %w", err)
	}
	if err = os.MkdirAll(s.authDir, 0o700); err != nil {
		return fmt.Errorf("mongo store: recreate auth directory: %w", err)
	}

	for cursor.Next(ctx) {
		var doc authDocument
		if err = cursor.Decode(&doc); err != nil {
			return fmt.Errorf("mongo store: decode auth document: %w", err)
		}
		path, errPath := s.absoluteAuthPath(doc.ID)
		if errPath != nil {
			log.WithError(errPath).Warnf("mongo store: skipping auth %s outside spool", doc.ID)
			continue
		}
		if err = os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			return fmt.Errorf("mongo store: create auth subdir: %w", err)
		}
		// Convert BSON to JSON for local file storage
		jsonData, errMarshal := bson.MarshalExtJSON(doc.Content, false, false)
		if errMarshal != nil {
			return fmt.Errorf("mongo store: marshal auth to json: %w", errMarshal)
		}
		if err = os.WriteFile(path, jsonData, 0o600); err != nil {
			return fmt.Errorf("mongo store: write auth file: %w", err)
		}
	}
	if err = cursor.Err(); err != nil {
		return fmt.Errorf("mongo store: iterate auth cursor: %w", err)
	}
	return nil
}

func (s *MongoStore) syncAuthFile(ctx context.Context, relID, path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return s.deleteAuthRecord(ctx, relID)
		}
		return fmt.Errorf("mongo store: read auth file: %w", err)
	}
	if len(data) == 0 {
		return s.deleteAuthRecord(ctx, relID)
	}
	return s.persistAuth(ctx, relID, data)
}

func (s *MongoStore) upsertAuthRecord(ctx context.Context, relID, path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("mongo store: read auth file: %w", err)
	}
	if len(data) == 0 {
		return s.deleteAuthRecord(ctx, relID)
	}
	return s.persistAuth(ctx, relID, data)
}

func (s *MongoStore) persistAuth(ctx context.Context, relID string, data []byte) error {
	// Parse JSON data to BSON for proper document storage
	var content bson.M
	if err := json.Unmarshal(data, &content); err != nil {
		return fmt.Errorf("mongo store: parse auth json: %w", err)
	}

	now := time.Now()
	filter := bson.M{"_id": relID}
	update := bson.M{
		"$set": bson.M{
			"content":    content,
			"updated_at": now,
		},
		"$setOnInsert": bson.M{
			"created_at": now,
		},
	}
	opts := options.Update().SetUpsert(true)
	if _, err := s.authColl.UpdateOne(ctx, filter, update, opts); err != nil {
		return fmt.Errorf("mongo store: upsert auth record: %w", err)
	}
	return nil
}

func (s *MongoStore) deleteAuthRecord(ctx context.Context, relID string) error {
	if _, err := s.authColl.DeleteOne(ctx, bson.M{"_id": relID}); err != nil {
		return fmt.Errorf("mongo store: delete auth record: %w", err)
	}
	return nil
}

func (s *MongoStore) persistConfig(ctx context.Context, data []byte) error {
	now := time.Now()
	normalized := normalizeLineEndings(string(data))
	filter := bson.M{"_id": defaultMongoConfigKey}
	update := bson.M{
		"$set": bson.M{
			"content":    normalized,
			"updated_at": now,
		},
		"$setOnInsert": bson.M{
			"created_at": now,
		},
	}
	opts := options.Update().SetUpsert(true)
	if _, err := s.configColl.UpdateOne(ctx, filter, update, opts); err != nil {
		return fmt.Errorf("mongo store: upsert config: %w", err)
	}
	return nil
}

func (s *MongoStore) deleteConfigRecord(ctx context.Context) error {
	if _, err := s.configColl.DeleteOne(ctx, bson.M{"_id": defaultMongoConfigKey}); err != nil {
		return fmt.Errorf("mongo store: delete config: %w", err)
	}
	return nil
}

func (s *MongoStore) resolveAuthPath(auth *cliproxyauth.Auth) (string, error) {
	if auth == nil {
		return "", fmt.Errorf("mongo store: auth is nil")
	}
	if auth.Attributes != nil {
		if p := strings.TrimSpace(auth.Attributes["path"]); p != "" {
			return p, nil
		}
	}
	if fileName := strings.TrimSpace(auth.FileName); fileName != "" {
		if filepath.IsAbs(fileName) {
			return fileName, nil
		}
		return filepath.Join(s.authDir, fileName), nil
	}
	if auth.ID == "" {
		return "", fmt.Errorf("mongo store: missing id")
	}
	if filepath.IsAbs(auth.ID) {
		return auth.ID, nil
	}
	return filepath.Join(s.authDir, filepath.FromSlash(auth.ID)), nil
}

func (s *MongoStore) resolveDeletePath(id string) (string, error) {
	if strings.ContainsRune(id, os.PathSeparator) || filepath.IsAbs(id) {
		return id, nil
	}
	return filepath.Join(s.authDir, filepath.FromSlash(id)), nil
}

func (s *MongoStore) relativeAuthID(path string) (string, error) {
	if s == nil {
		return "", fmt.Errorf("mongo store: store not initialized")
	}
	if !filepath.IsAbs(path) {
		path = filepath.Join(s.authDir, path)
	}
	clean := filepath.Clean(path)
	rel, err := filepath.Rel(s.authDir, clean)
	if err != nil {
		return "", fmt.Errorf("mongo store: compute relative path: %w", err)
	}
	if strings.HasPrefix(rel, "..") {
		return "", fmt.Errorf("mongo store: path %s outside managed directory", path)
	}
	return filepath.ToSlash(rel), nil
}

func (s *MongoStore) absoluteAuthPath(id string) (string, error) {
	if s == nil {
		return "", fmt.Errorf("mongo store: store not initialized")
	}
	clean := filepath.Clean(filepath.FromSlash(id))
	if strings.HasPrefix(clean, "..") {
		return "", fmt.Errorf("mongo store: invalid auth identifier %s", id)
	}
	path := filepath.Join(s.authDir, clean)
	rel, err := filepath.Rel(s.authDir, path)
	if err != nil {
		return "", err
	}
	if strings.HasPrefix(rel, "..") {
		return "", fmt.Errorf("mongo store: resolved auth path escapes auth directory")
	}
	return path, nil
}

// copyConfigTemplate copies a config template file to the destination path.
func copyConfigTemplate(src, dst string) error {
	data, err := os.ReadFile(src)
	if err != nil {
		return err
	}
	if err = os.MkdirAll(filepath.Dir(dst), 0o700); err != nil {
		return err
	}
	return os.WriteFile(dst, data, 0o600)
}
