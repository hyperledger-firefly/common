// Copyright © 2025 Kaleido, Inc.
//
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package config

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/hyperledger-firefly/common/pkg/fftypes"
	"github.com/hyperledger-firefly/common/pkg/fswatcher"
	"github.com/hyperledger-firefly/common/pkg/i18n"
	"github.com/hyperledger-firefly/common/pkg/log"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"

	lumberjack "gopkg.in/natefinch/lumberjack.v2"
)

var ffc = AddRootKey

// The following keys can be access from the root configuration.
// Plugins are responsible for defining their own keys using the Config interface
var (
	// Lang is the language to use for translation
	Lang = ffc("lang")
	// LogForceColor forces color to be enabled, even if we do not detect a TTY
	LogForceColor = ffc("log.forceColor")
	// LogLevel is the logging level
	LogLevel = ffc("log.level")
	// LogNoColor forces color to be disabled, even if we detect a TTY
	LogNoColor = ffc("log.noColor")
	// LogTimeFormat is a string format for timestamps
	LogTimeFormat = ffc("log.timeFormat")
	// LogUTC sets log timestamps to the UTC timezone
	LogUTC = ffc("log.utc")
	// LogFilename sets logging to file
	LogFilename = ffc("log.filename")
	// LogFilesize sets the size to roll logs at
	LogFilesize = ffc("log.filesize")
	// LogMaxBackups sets the maximum number of old files to keep
	LogMaxBackups = ffc("log.maxBackups")
	// LogMaxAge sets the maximum age at which to roll
	LogMaxAge = ffc("log.maxAge")
	// LogCompress sets whether to compress backups
	LogCompress = ffc("log.compress")
	// LogIncludeCodeInfo enables the report caller for including the calling file and line number
	LogIncludeCodeInfo = ffc("log.includeCodeInfo")
	// LogJSONEnabled enables JSON formatted logs rather than text
	LogJSONEnabled = ffc("log.json.enabled")
	// LogJSONTimestampField configures the JSON key containing the timestamp of the log
	LogJSONTimestampField = ffc("log.json.fields.timestamp")
	// LogJSONLevelField configures the JSON key containing the log level
	LogJSONLevelField = ffc("log.json.fields.level")
	// LogJSONMessageField configures the JSON key containing the log message
	LogJSONMessageField = ffc("log.json.fields.message")
	// LogJSONFuncField configures the JSON key containing the calling function
	LogJSONFuncField = ffc("log.json.fields.func")
	// LogJSONFileField configures the JSON key containing the calling file
	LogJSONFileField = ffc("log.json.fields.file")
)

type KeySet interface {
	AddKnownKey(key string, defValue ...interface{})
}

type sectionParent interface {
	AddChild(key string, defValue ...interface{})
}

// Section represents a section of the global configuration, at a nested point in the config hierarchy.
// Note that all keys are added to a GLOBAL map, so this cannot be used for per-instance customization.
type Section interface {
	KeySet
	SetDefault(key string, defValue interface{})
	SubSection(name string) Section
	SubArray(name string) ArraySection
	Set(key string, value interface{})
	IsSet(key string) bool
	Resolve(key string) string

	GetString(key string) string
	GetBool(key string) bool
	GetInt(key string) int
	GetInt64(key string) int64
	GetFloat64(key string) float64
	GetByteSize(key string) int64
	GetUint(key string) uint
	GetUint64(key string) uint64
	GetDuration(key string) time.Duration
	GetStringSlice(key string) []string
	GetObject(key string) fftypes.JSONObject
	GetObjectArray(key string) fftypes.JSONObjectArray
	Get(key string) interface{}
	GetBigInt(key string) *big.Int
}

// ArraySection represents an array of options at a particular layer in the config.
// This allows specifying the schema of keys that exist for every entry, and the defaults,
// as well as querying how many entries exist and iterating through them.
type ArraySection interface {
	KeySet
	ArraySize() int
	ArrayEntry(i int) Section
	SubSection(name string) Section
	SubArray(name string) ArraySection
	SetDefault(key string, defValue interface{})
}

// RootKey key are the known configuration keys
type RootKey string

func RootConfigReset(setServiceDefaults ...func()) {
	keysMutex.Lock() // must only call viper directly here (as we already hold the lock)
	defer keysMutex.Unlock()

	// We retain the config file used during a reset, to facility dynamic reload scenarios
	oldCfgFile := viper.ConfigFileUsed()
	viper.Reset()
	viper.SetConfigFile(oldCfgFile)

	viper.SetDefault(string(Lang), "en")
	viper.SetDefault(string(LogLevel), "info")
	viper.SetDefault(string(LogTimeFormat), "2006-01-02T15:04:05.000Z07:00")
	viper.SetDefault(string(LogUTC), false)
	viper.SetDefault(string(LogFilesize), "100m")
	viper.SetDefault(string(LogMaxAge), "24h")
	viper.SetDefault(string(LogMaxBackups), 2)
	viper.SetDefault(string(LogIncludeCodeInfo), false)
	viper.SetDefault(string(LogJSONEnabled), false)
	viper.SetDefault(string(LogJSONTimestampField), "@timestamp")
	viper.SetDefault(string(LogJSONLevelField), "level")
	viper.SetDefault(string(LogJSONMessageField), "message")
	viper.SetDefault(string(LogJSONFuncField), "func")
	viper.SetDefault(string(LogJSONFileField), "file")

	// We set the service defaults within our mutex
	for _, fn := range setServiceDefaults {
		fn()
	}

	i18n.SetLang(viper.GetString(string(Lang)))
}

var envPrefix = "firefly"

func SetEnvPrefix(prefix string) {
	envPrefix = prefix
}

// ReadConfig initializes the config
func ReadConfig(cfgSuffix, cfgFile string) error {
	keysMutex.Lock() // must only call viper directly here (as we already hold the lock)
	defer keysMutex.Unlock()

	// Set precedence order for reading config location
	viper.SetEnvPrefix(envPrefix)
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.AutomaticEnv()
	viper.SetConfigType("yaml")
	viper.SetConfigName(fmt.Sprintf("firefly.%s", cfgSuffix))
	viper.AddConfigPath("/etc/firefly/")
	viper.AddConfigPath("$HOME/.firefly")
	viper.AddConfigPath(".")
	if cfgFile != "" {
		viper.SetConfigFile(cfgFile)
	}
	return viper.ReadInConfig()
}

// Listens for changes to the configuration file configured in Viper.
func WatchConfig(ctx context.Context, onChange, onClose func()) error {

	// Note that while viper.WatchConfig() exists, it doesn't handle various
	// types of config change. Specifically it doesn't handle the case where the
	// removed event comes from the filesystem, before the create/update event(s).
	// The listener just ends in that case, and stops notifying of events :shrug

	fullConfigFilePath := viper.ConfigFileUsed()
	return fswatcher.Watch(ctx, fullConfigFilePath, onChange, onClose)
}

func MergeConfig(configRecords []*fftypes.ConfigRecord) error {
	keysMutex.Lock()
	defer keysMutex.Unlock()

	for _, c := range configRecords {
		s := viper.New()
		s.SetConfigType("json")
		var val interface{}
		if c.Value != nil {
			if err := json.Unmarshal([]byte(*c.Value), &val); err != nil {
				return err
			}
		}
		switch v := val.(type) {
		case map[string]interface{}:
			_ = s.ReadConfig(bytes.NewBuffer([]byte(*c.Value)))
			for _, k := range s.AllKeys() {
				value := s.Get(k)
				if reflect.TypeOf(value).Kind() == reflect.Slice {
					configSlice := value.([]interface{})
					for i := range configSlice {
						viper.Set(fmt.Sprintf("%s.%s.%d", c.Key, k, i), configSlice[i])
					}
				} else {
					viper.Set(fmt.Sprintf("%s.%s", c.Key, k), value)
				}
			}
		case []interface{}:
			_ = s.ReadConfig(bytes.NewBuffer([]byte(*c.Value)))
			for i := range v {
				viper.Set(fmt.Sprintf("%s.%d", c.Key, i), v[i])
			}
		default:
			viper.Set(c.Key, v)
		}
	}
	return nil
}

var knownKeys = map[string]bool{} // All config keys go here, including those defined in sub-sections
var keysMutex sync.Mutex
var root = &configSection{}

// AddRootKey adds a root key, used to define the keys that are used within the core
func AddRootKey(k string) RootKey {
	root.AddKnownKey(k)
	return RootKey(k)
}

// GetKnownKeys gets the known keys
func GetKnownKeys() []string {
	keysMutex.Lock()
	defer keysMutex.Unlock()

	keys := make([]string, 0, len(knownKeys))
	for k := range knownKeys {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// configSection is the main config structure passed to plugins, and used for root to wrap viper
type configSection struct {
	prefix     string
	parent     sectionParent
	arrayEntry bool
}

// configArray is a point in the config that supports an array
type configArray struct {
	base     string
	parent   sectionParent
	defaults map[string][]interface{}
}

// RootSection creates a new configuration section starting from the config root
func RootSection(prefix string) Section {
	return &configSection{
		prefix: strings.TrimSuffix(prefix, "."),
	}
}

// RootArray creates a new configuration array starting from the config root
func RootArray(prefix string) ArraySection {
	return &configArray{
		base:     strings.TrimSuffix(prefix, "."),
		defaults: make(map[string][]interface{}),
	}
}

func keyName(prefix, name string) string {
	if prefix != "" {
		return prefix + "." + name
	}
	return name
}

func (c *configSection) prefixKey(k string) string {
	// Caller responsible for holding lock when calling
	key := keyName(c.prefix, k)
	if !knownKeys[key] {
		panic(fmt.Sprintf("Undefined configuration key '%s'", key))
	}
	return key
}

func (c *configSection) SubSection(name string) Section {
	return &configSection{
		prefix: keyName(c.prefix, name),
		parent: c,
	}
}

func (c *configArray) SubSection(name string) Section {
	cp := &configSection{
		prefix: keyName(c.base+"[]", name),
		parent: c,
	}
	return cp
}

// If any parent is an array entry, retrieve fully-qualified defaults via its array parent
func getArrayEntryDefaults(p sectionParent) map[string][]interface{} {
	switch c := p.(type) {
	case *configArray:
		return getArrayEntryDefaults(c.parent)
	case *configSection:
		if !c.arrayEntry {
			return getArrayEntryDefaults(c.parent)
		}
		arrayParent := c.parent.(*configArray)
		defaults := make(map[string][]interface{})
		for k, v := range arrayParent.defaults {
			defaults[c.prefix+"."+k] = v
		}
		return defaults
	default:
		return nil
	}
}

func (c *configSection) SubArray(name string) ArraySection {
	a := &configArray{
		base:     keyName(c.prefix, name),
		parent:   c,
		defaults: make(map[string][]interface{}),
	}
	// Get defaults from any enclosing array entry, and copy over any applicable to this subtree
	// This is necessary to propagate known keys for arrays within arrays
	prefix := a.base + "[]."
	for key, val := range getArrayEntryDefaults(c) {
		if strings.HasPrefix(key, prefix) {
			a.defaults[strings.TrimPrefix(key, prefix)] = val
		}
	}
	return a
}

func (c *configArray) SubArray(name string) ArraySection {
	return &configArray{
		base:     keyName(c.base+"[]", name),
		parent:   c,
		defaults: make(map[string][]interface{}),
	}
}

func (c *configArray) ArraySize() int {
	val := viper.Get(c.base)
	vt := reflect.TypeOf(val)
	if vt != nil && (vt.Kind() == reflect.Slice || vt.Kind() == reflect.Map) {
		return reflect.ValueOf(val).Len()
	}
	return 0
}

// ArrayEntry must only be called after the config has been loaded
func (c *configArray) ArrayEntry(i int) Section {
	cp := &configSection{
		prefix:     keyName(c.base, fmt.Sprintf("%d", i)),
		parent:     c,
		arrayEntry: true,
	}
	for knownKey, defValue := range c.defaults {
		cp.AddKnownKey(knownKey, defValue...)
		// Sadly Viper can't handle defaults inside the array, when
		// a value is set. So here we check/set the defaults.
		if defValue != nil && cp.Get(knownKey) == nil {
			if len(defValue) == 1 {
				cp.Set(knownKey, defValue[0])
			} else if len(defValue) > 0 {
				cp.Set(knownKey, defValue)
			}
		}
	}
	return cp
}

func (c *configArray) AddKnownKey(k string, defValue ...interface{}) {
	keysMutex.Lock()
	defer keysMutex.Unlock()

	// Put a simulated key in the known keys array, to pop into the help info.
	key := keyName(c.base+"[]", k)
	knownKeys[key] = true

	c.AddChild(key, defValue...)
}

func (c *configSection) AddKnownKey(k string, defValue ...interface{}) {
	key := keyName(c.prefix, k)
	if len(defValue) == 1 {
		c.SetDefault(k, defValue[0])
	} else if len(defValue) > 0 {
		c.SetDefault(k, defValue)
	}
	keysMutex.Lock()
	defer keysMutex.Unlock()
	knownKeys[key] = true

	if c.parent != nil {
		c.parent.AddChild(key, defValue...)
	}
}

func (c *configArray) AddChild(k string, defValue ...interface{}) {
	// When a child is added anywhere below this array, add it to the defaults map
	prefix := c.base + "[]."
	if strings.HasPrefix(k, prefix) {
		c.defaults[strings.TrimPrefix(k, prefix)] = defValue
	}

	// Also bubble it upwards
	if c.parent != nil {
		c.parent.AddChild(k, defValue...)
	}
}

func (c *configSection) AddChild(k string, defValue ...interface{}) {
	// When a child is added anywhere below this key, just bubble it upwards
	if c.parent != nil {
		c.parent.AddChild(k, defValue...)
	}
}

func (c *configSection) SetDefault(k string, defValue interface{}) {
	key := keyName(c.prefix, k)
	viper.SetDefault(key, defValue)
	c.AddChild(key, defValue)
}

func (c *configArray) SetDefault(k string, defValue interface{}) {
	key := keyName(c.base+"[]", k)
	viper.SetDefault(key, defValue)
	c.AddChild(key, defValue)
}

func GetConfig() fftypes.JSONObject {
	keysMutex.Lock()
	defer keysMutex.Unlock()

	conf := fftypes.JSONObject{}
	_ = viper.Unmarshal(&conf)
	return conf
}

// GetString gets a configuration string
func GetString(key RootKey) string {
	return root.GetString(string(key))
}
func (c *configSection) GetString(key string) string {
	keysMutex.Lock()
	defer keysMutex.Unlock()

	return viper.GetString(c.prefixKey(key))
}

// GetStringSlice gets a configuration string array
func GetStringSlice(key RootKey) []string {
	return root.GetStringSlice(string(key))
}
func (c *configSection) GetStringSlice(key string) []string {
	keysMutex.Lock()
	defer keysMutex.Unlock()

	return viper.GetStringSlice(c.prefixKey(key))
}

// GetBool gets a configuration bool
func GetBool(key RootKey) bool {
	return root.GetBool(string(key))
}
func (c *configSection) GetBool(key string) bool {
	keysMutex.Lock()
	defer keysMutex.Unlock()

	return viper.GetBool(c.prefixKey(key))
}

// GetDuration gets a configuration time duration with consistent semantics
func GetDuration(key RootKey) time.Duration {
	return root.GetDuration(string(key))
}
func (c *configSection) GetDuration(key string) time.Duration {
	keysMutex.Lock()
	defer keysMutex.Unlock()

	return fftypes.ParseToDuration(viper.GetString(c.prefixKey(key)))
}

// GetByteSize get a size in bytes
func GetByteSize(key RootKey) int64 {
	return root.GetByteSize(string(key))
}
func (c *configSection) GetByteSize(key string) int64 {
	keysMutex.Lock()
	defer keysMutex.Unlock()

	return fftypes.ParseToByteSize(viper.GetString(c.prefixKey(key)))
}

// GetBigInt gets a configuration big integer.
// Uses base 0 so that the string can be in decimal, hexadecimal (0x...), octal (0o...), or binary (0b...) formats.
// See math/big.Int.SetString docs for details.
func GetBigInt(key RootKey) *big.Int {
	return root.GetBigInt(string(key))
}
func (c *configSection) GetBigInt(key string) *big.Int {
	keysMutex.Lock()
	defer keysMutex.Unlock()
	bigIntStr := viper.GetString(c.prefixKey(key))
	valueStr := strings.TrimSpace(bigIntStr)
	if valueStr == "" {
		return nil
	}

	value := &big.Int{}
	_, ok := value.SetString(valueStr, 0)
	if !ok {
		log.L(context.Background()).Warnf("Unable to parse string %s into big.Int", valueStr)
		return nil
	}
	return value
}

// GetUint gets a configuration uint
func GetUint(key RootKey) uint {
	return root.GetUint(string(key))
}
func (c *configSection) GetUint(key string) uint {
	keysMutex.Lock()
	defer keysMutex.Unlock()

	return viper.GetUint(c.prefixKey(key))
}

// GetInt gets a configuration uint
func GetInt(key RootKey) int {
	return root.GetInt(string(key))
}
func (c *configSection) GetInt(key string) int {
	keysMutex.Lock()
	defer keysMutex.Unlock()

	return viper.GetInt(c.prefixKey(key))
}

// GetInt64 gets a configuration int64
func GetInt64(key RootKey) int64 {
	return root.GetInt64(string(key))
}
func (c *configSection) GetInt64(key string) int64 {
	keysMutex.Lock()
	defer keysMutex.Unlock()

	return viper.GetInt64(c.prefixKey(key))
}

// GetUint64 gets a configuration uint64
func GetUint64(key RootKey) uint64 {
	return root.GetUint64(string(key))
}
func (c *configSection) GetUint64(key string) uint64 {
	keysMutex.Lock()
	defer keysMutex.Unlock()

	return viper.GetUint64(c.prefixKey(key))
}

// GetFloat64 gets a configuration uint
func GetFloat64(key RootKey) float64 {
	return root.GetFloat64(string(key))
}
func (c *configSection) GetFloat64(key string) float64 {
	keysMutex.Lock()
	defer keysMutex.Unlock()

	return viper.GetFloat64(c.prefixKey(key))
}

// GetObject gets a configuration map
func GetObject(key RootKey) fftypes.JSONObject {
	return root.GetObject(string(key))
}
func (c *configSection) GetObject(key string) fftypes.JSONObject {
	keysMutex.Lock()
	defer keysMutex.Unlock()

	return fftypes.JSONObject(viper.GetStringMap(c.prefixKey(key)))
}

// GetObjectArray gets an array of configuration maps
func GetObjectArray(key RootKey) fftypes.JSONObjectArray {
	return root.GetObjectArray(string(key))
}
func (c *configSection) GetObjectArray(key string) fftypes.JSONObjectArray {
	keysMutex.Lock()
	defer keysMutex.Unlock()

	v, _ := fftypes.ToJSONObjectArray(viper.Get(c.prefixKey(key)))
	return v
}

// Get gets a configuration in raw form
func Get(key RootKey) interface{} {
	return root.Get(string(key))
}
func (c *configSection) Get(key string) interface{} {
	keysMutex.Lock()
	defer keysMutex.Unlock()

	return viper.Get(c.prefixKey(key))
}

// Set allows runtime setting of config (used in unit tests)
func Set(key RootKey, value interface{}) {
	root.Set(string(key), value)
}
func (c *configSection) Set(key string, value interface{}) {
	keysMutex.Lock()
	defer keysMutex.Unlock()

	viper.Set(c.prefixKey(key), value)
}

// Resolve gives the fully qualified path of a key
func (c *configSection) Resolve(key string) string {
	keysMutex.Lock()
	defer keysMutex.Unlock()

	return c.prefixKey(key)
}

func IsSet(key RootKey) bool {
	return root.IsSet(string(key))
}

// IsSet returns true when a key has non-default value set
func (c *configSection) IsSet(key string) bool {
	keysMutex.Lock()
	defer keysMutex.Unlock()

	return viper.IsSet(c.prefixKey(key))
}

// SetupLogging initializes logging
func SetupLogging(ctx context.Context) {
	log.SetFormatting(log.Formatting{
		DisableColor:       GetBool(LogNoColor),
		ForceColor:         GetBool(LogForceColor),
		TimestampFormat:    GetString(LogTimeFormat),
		UTC:                GetBool(LogUTC),
		IncludeCodeInfo:    GetBool(LogIncludeCodeInfo),
		JSONEnabled:        GetBool(LogJSONEnabled),
		JSONTimestampField: GetString(LogJSONTimestampField),
		JSONLevelField:     GetString(LogJSONLevelField),
		JSONMessageField:   GetString(LogJSONMessageField),
		JSONFuncField:      GetString(LogJSONFuncField),
		JSONFileField:      GetString(LogJSONFileField),
	})
	logFilename := GetString(LogFilename)
	if logFilename != "" {
		lumberjack := &lumberjack.Logger{
			Filename:   logFilename,
			MaxSize:    int(math.Ceil(float64(GetByteSize(LogFilesize)) / 1024 / 1024)), /* round up in megabytes */
			MaxBackups: GetInt(LogMaxBackups),
			MaxAge:     int(math.Ceil(float64(GetDuration(LogMaxAge)) / float64(time.Hour) / 24)), /* round up in days */
			Compress:   GetBool(LogCompress),
		}
		logrus.SetOutput(lumberjack)
	}
	log.SetLevel(GetString(LogLevel))
	log.L(ctx).Debugf("Log level: %s", logrus.GetLevel())
}

func GenerateConfigMarkdown(ctx context.Context, header string, keys []string) ([]byte, error) {
	b := bytes.NewBuffer([]byte{})

	rootKeyHeaderLevel := 2

	b.WriteString(header)

	configObjects := make(map[string][]string)
	configObjectNames := make([]string, 0)

	for _, fullKey := range keys {
		splitKey := strings.Split(fullKey, ".")
		if len(splitKey) > 1 {
			configObjectName := strings.Join(splitKey[:len(splitKey)-1], ".")
			keyName := splitKey[len(splitKey)-1]
			if _, ok := configObjects[configObjectName]; !ok {
				configObjects[configObjectName] = make([]string, 0)
				configObjectNames = append(configObjectNames, configObjectName)
			}
			configObjects[configObjectName] = append(configObjects[configObjectName], keyName)
		}
	}
	sort.Strings(configObjectNames)
	for _, configObjectName := range configObjectNames {
		rowsInTable := []string{}

		sort.Strings(configObjects[configObjectName])
		for _, key := range configObjects[configObjectName] {
			fullKey := fmt.Sprintf("%s.%s", configObjectName, key)
			description, fieldType := getDescriptionForConfigKey(ctx, fullKey)
			if fieldType != i18n.IgnoredType {
				row := fmt.Sprintf("\n|%s|%s|%s|`%v`", key, description, fieldType, Get(RootKey(fullKey)))
				rowsInTable = append(rowsInTable, row)
			}
		}
		if len(rowsInTable) > 0 {
			b.WriteString(fmt.Sprintf("\n\n%s %s", strings.Repeat("#", rootKeyHeaderLevel), configObjectName))
			b.WriteString("\n\n|Key|Description|Type|Default Value|")
			b.WriteString("\n|---|-----------|----|-------------|")
			for _, row := range rowsInTable {
				b.WriteString(row)
			}
		}
	}
	return b.Bytes(), nil
}

func getDescriptionForConfigKey(ctx context.Context, key string) (string, string) {
	configDescriptionKey := "config." + key
	description := i18n.Expand(ctx, i18n.MessageKey(configDescriptionKey))
	fieldType, ok := i18n.GetFieldType(configDescriptionKey)
	if description != configDescriptionKey && ok {
		return description, fieldType
	}
	return getGlobalDescriptionforConfigKey(ctx, key)
}

func getGlobalDescriptionforConfigKey(ctx context.Context, key string) (string, string) {
	// No specific description was found, look for a global
	splitKey := strings.Split(key, ".")
	// Walk through the key structure starting with the most specific key possible, working to the most generic
	for i := 0; i < len(splitKey); i++ {
		configDescriptionKey := "config.global." + strings.Join(splitKey[i:], ".")
		description := i18n.Expand(ctx, i18n.MessageKey(configDescriptionKey))
		fieldType, ok := i18n.GetFieldType(configDescriptionKey)
		if description != configDescriptionKey && ok {
			return description, fieldType
		}
	}
	panic(fmt.Sprintf("Translation for config key '%s' was not found", key))
}
