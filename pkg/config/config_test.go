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
	"context"
	"fmt"
	"math/big"
	"os"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/hyperledger-firefly/common/pkg/fftypes"
	"github.com/hyperledger-firefly/common/pkg/i18n"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"golang.org/x/text/language"
)

const configDir = "../../test/data/config"

func TestInitConfigOK(t *testing.T) {
	viper.Reset()
	err := ReadConfig("core", "")
	assert.Regexp(t, "Not Found", err)
}

func TestDefaults(t *testing.T) {
	cwd, err := os.Getwd()
	assert.NoError(t, err)
	os.Chdir(configDir)
	defer os.Chdir(cwd)

	key1 := AddRootKey("key1")
	key2 := AddRootKey("key3")
	key3 := AddRootKey("key3")
	key4 := AddRootKey("key4")
	key5 := AddRootKey("key5")
	key6 := AddRootKey("key6")
	key7 := AddRootKey("key7")
	key8 := AddRootKey("key8")
	key9 := AddRootKey("key9")

	RootConfigReset(func() {
		viper.SetDefault(string(key1), "value1")
		viper.SetDefault(string(key2), true)
		viper.SetDefault(string(key3), 25)
		viper.SetDefault(string(key5), "250ms")
		viper.SetDefault(string(key6), "2.0")
		viper.SetDefault(string(key7), []string{"value1", "value2"})
		viper.SetDefault(string(key8), fftypes.JSONObjectArray{{"key1": "value1"}})
		viper.SetDefault(string(key9), "1Mb")
	})
	err = ReadConfig("common", "")
	assert.NoError(t, err)

	assert.Equal(t, "value1", GetString(key1))
	assert.True(t, GetBool(key2))
	assert.Equal(t, uint(25), GetUint(key3))
	assert.Equal(t, int(0), GetInt(key4))
	assert.Equal(t, int64(0), GetInt64(key4))
	assert.Equal(t, uint64(0), GetUint64(key4))
	assert.Equal(t, 250*time.Millisecond, GetDuration(key5))
	assert.Equal(t, float64(2.0), GetFloat64(key6))
	assert.Equal(t, []string{"value1", "value2"}, GetStringSlice(key7))
	assert.NotEmpty(t, GetObjectArray(key8))
	assert.Equal(t, int64(1024*1024), GetByteSize(key9))
}

func TestValueSet(t *testing.T) {
	RootConfigReset()

	// keys with default values are not set
	AddRootKey("key1")
	assert.False(t, IsSet("key1"))
	Set("key1", "updatedvalue")
	assert.True(t, IsSet("key1"))

	section := RootSection("child")
	section.AddKnownKey("key1")
	assert.False(t, section.IsSet("key1"))
	section.Set("key1", "updatedvalue")
	assert.True(t, section.IsSet("key1"))
}

func TestSpecificConfigFileOk(t *testing.T) {
	RootConfigReset()
	err := ReadConfig("common", configDir+"/firefly.common.yaml")
	assert.NoError(t, err)
}

func TestSpecificConfigFileFail(t *testing.T) {
	RootConfigReset()
	err := ReadConfig("core", configDir+"/no.hope.yaml")
	assert.Error(t, err)
}

func TestAttemptToAccessRandomKey(t *testing.T) {
	assert.Panics(t, func() {
		GetString("any.key")
	})
}

func TestSetGetMap(t *testing.T) {
	defer RootConfigReset()
	key1 := AddRootKey("key1")
	Set(key1, map[string]interface{}{"some": "map"})
	assert.Equal(t, fftypes.JSONObject{"some": "map"}, GetObject(key1))
}

func TestSetGetRawInterace(t *testing.T) {
	defer RootConfigReset()
	type myType struct{ name string }
	key1 := AddRootKey("key1")
	Set(key1, &myType{name: "test"})
	v := Get(key1)
	assert.Equal(t, myType{name: "test"}, *(v.(*myType)))
}

func TestGetBadDurationMillisDefault(t *testing.T) {
	defer RootConfigReset()
	key1 := AddRootKey("key1")
	Set(key1, "12345")
	assert.Equal(t, time.Duration(12345)*time.Millisecond, GetDuration(key1))
}

func TestGetBadDurationZero(t *testing.T) {
	defer RootConfigReset()
	key1 := AddRootKey("key1")
	Set(key1, "!a number or duration")
	assert.Equal(t, time.Duration(0), GetDuration(key1))
}

func TestPluginConfig(t *testing.T) {
	pic := RootSection("my")
	pic.AddKnownKey("special.config", 12345)
	assert.Equal(t, 12345, pic.GetInt("special.config"))
}

func TestPluginConfigArrayInit(t *testing.T) {
	pic := RootSection("my").SubSection("special")
	pic.AddKnownKey("config", "val1", "val2", "val3")
	assert.Equal(t, []string{"val1", "val2", "val3"}, pic.GetStringSlice("config"))
}

func TestArrayOfPlugins(t *testing.T) {
	defer RootConfigReset()

	pluginsRoot := RootSection("plugins")
	tokPlugins := pluginsRoot.SubArray("tokens")
	tokPlugins.AddKnownKey("fftokens")
	tokPlugins.AddKnownKey("name")
	tokPlugins.AddKnownKey("key1", "default value")
	tokPlugins.AddKnownKey("key2", "def1", "def2")
	fft := tokPlugins.SubSection("fftokens")
	fft.AddKnownKey("url")
	fft.SubSection("proxy").AddKnownKey("url")
	fft.AddKnownKey("testdefault")
	fft.SetDefault("testdefault", "test")
	headers := fft.SubArray("headers")
	headers.AddKnownKey("name", "defname")
	viper.SetConfigType("yaml")
	err := viper.ReadConfig(strings.NewReader(`
plugins:
  tokens:
  - name: bob
    type: fftokens
    fftokens:
      url: http://test
      proxy:
        url: http://proxy
      headers:
      - name: header1
      - badkey: bad
`))
	assert.NoError(t, err)
	assert.Equal(t, 1, tokPlugins.ArraySize())
	assert.Equal(t, 0, RootArray("nonexistent").ArraySize())
	bob := tokPlugins.ArrayEntry(0)
	assert.Equal(t, "bob", bob.GetString("name"))
	assert.Equal(t, "default value", bob.GetString("key1"))
	assert.Equal(t, "http://test", bob.GetString("fftokens.url"))
	bobfft := bob.SubSection("fftokens")
	assert.Equal(t, "http://test", bobfft.GetString("url"))
	assert.Equal(t, "http://proxy", bobfft.SubSection("proxy").GetString("url"))
	assert.Equal(t, "test", bobfft.GetString("testdefault"))
	bobheaders := bobfft.SubArray("headers")
	assert.Equal(t, 2, bobheaders.ArraySize())
	assert.Equal(t, "header1", bobheaders.ArrayEntry(0).GetString("name"))
	assert.Equal(t, "defname", bobheaders.ArrayEntry(1).GetString("name"))
}

func TestNestedArrays(t *testing.T) {
	defer RootConfigReset()

	namespacesRoot := RootSection("namespaces")
	predefined := namespacesRoot.SubArray("predefined")
	predefined.AddKnownKey("name")
	predefined.AddKnownKey("key1", "default value")
	tlsConfigs := predefined.SubArray("tlsConfigs")
	tlsConfigs.SetDefault("testdefault", "test")
	tlsConfigs.AddKnownKey("name")
	tlsConfigs.SubSection("tls").AddKnownKey("enabled")
	tlsConfigs.SubSection("tls").SetDefault("testdefault", "test")
	viper.SetConfigType("yaml")
	err := viper.ReadConfig(strings.NewReader(`
namespaces:
  predefined:
  - name: myns
    tlsConfigs:
    - name: myconfig
      tls:
        enabled: true
`))
	assert.NoError(t, err)
	assert.Equal(t, 1, predefined.ArraySize())
	assert.Equal(t, 0, RootArray("nonexistent").ArraySize())
	ns := predefined.ArrayEntry(0)
	assert.Equal(t, "myns", ns.GetString("name"))
	assert.Equal(t, "default value", ns.GetString("key1"))
	nsTLSConfigs := ns.SubArray("tlsConfigs")
	assert.Equal(t, 1, nsTLSConfigs.ArraySize())
	tlsConfig := nsTLSConfigs.ArrayEntry(0)
	assert.Equal(t, "myconfig", tlsConfig.GetString("name"))
	assert.Equal(t, "test", tlsConfig.GetString("testdefault"))
	assert.Equal(t, true, tlsConfig.SubSection("tls").GetBool("enabled"))
	assert.Equal(t, "test", tlsConfig.SubSection("tls").GetString("testdefault"))
}
func TestNestedArraysObjectDefault(t *testing.T) {
	defer RootConfigReset()

	namespacesRoot := RootSection("namespaces")
	predefined := namespacesRoot.SubArray("predefined")
	predefined.AddKnownKey("name")
	predefined.AddKnownKey("key1", "default value")
	newSection := predefined.SubSection("new")
	tlsConfigs := newSection.SubArray("tlsConfigs")
	tlsConfigs.SetDefault("testdefault", "test")
	tlsConfigs.AddKnownKey("name")
	tlsConfigs.SubSection("tls").AddKnownKey("enabled")
	tlsConfigs.SubSection("tls").SetDefault("testdefault", "test")
	viper.SetConfigType("yaml")
	// thing.stuff[].watsit.hoojars[].mydefault

	err := viper.ReadConfig(strings.NewReader(`
namespaces:
  predefined:
  - name: myns
    new:
      tlsConfigs:
      - name: myconfig
        tls:
          enabled: true
`))
	assert.NoError(t, err)
	assert.Equal(t, 1, predefined.ArraySize())
	assert.Equal(t, 0, RootArray("nonexistent").ArraySize())
	ns := predefined.ArrayEntry(0)
	assert.Equal(t, "myns", ns.GetString("name"))
	assert.Equal(t, "default value", ns.GetString("key1"))
	section := ns.SubSection("new")
	nsTLSConfigs := section.SubArray("tlsConfigs")
	assert.Equal(t, 1, nsTLSConfigs.ArraySize())
	tlsConfig := nsTLSConfigs.ArrayEntry(0)
	assert.Equal(t, "myconfig", tlsConfig.GetString("name"))
	assert.Equal(t, "test", tlsConfig.GetString("testdefault"))
	assert.Equal(t, true, tlsConfig.SubSection("tls").GetBool("enabled"))
	assert.Equal(t, "test", tlsConfig.SubSection("tls").GetString("testdefault"))
}

func TestMapOfAdminOverridePlugins(t *testing.T) {
	defer RootConfigReset()

	tokPlugins := RootArray("tokens")
	tokPlugins.AddKnownKey("firstkey")
	tokPlugins.AddKnownKey("secondkey")
	viper.SetConfigType("json")
	err := viper.ReadConfig(strings.NewReader(`{
		"tokens": {
			"0": {
				"firstkey": "firstitemfirstkeyvalue",
				"secondkey": "firstitemsecondkeyvalue"
			},
			"1": {
				"firstkey": "seconditemfirstkeyvalue",
				"secondkey": "seconditemsecondkeyvalue"
			}
		}
	}`))
	assert.NoError(t, err)
	assert.Equal(t, 2, tokPlugins.ArraySize())
	assert.Equal(t, "firstitemfirstkeyvalue", tokPlugins.ArrayEntry(0).Get("firstkey"))
	assert.Equal(t, "seconditemsecondkeyvalue", tokPlugins.ArrayEntry(1).Get("secondkey"))
}

func TestGetKnownKeys(t *testing.T) {
	knownKeys := GetKnownKeys()
	assert.NotEmpty(t, knownKeys)
	for _, k := range knownKeys {
		assert.NotEmpty(t, root.Resolve(k))
	}
}

func TestSetupLoggingToFile(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "logtest")
	assert.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	fileName := path.Join(tmpDir, "test.log")
	RootConfigReset()
	Set(LogFilename, fileName)
	Set(LogLevel, "debug")
	Set(LogMaxAge, "72h")
	SetupLogging(context.Background())

	RootConfigReset()
	SetupLogging(context.Background())

	b, err := os.ReadFile(fileName)
	assert.NoError(t, err)
	assert.Regexp(t, "Log level", string(b))
}

func TestSetupLogging(t *testing.T) {
	SetupLogging(context.Background())
}

func TestMergeConfigOk(t *testing.T) {

	conf1 := fftypes.JSONAnyPtr(`{
		"some":  {
			"nested": {
				"stuff": "value1"
			}
		}
	}`)
	confNumber := fftypes.JSONAnyPtr(`{
		"some":  {
			"more": {
				"stuff": 15
			}
		}
	}`)
	conf3 := fftypes.JSONAnyPtr(`"value3"`)
	confNestedSlice := fftypes.JSONAnyPtr(`{
		"nestedslice": [
			{
				"firstitemfirstkey": "firstitemfirstkeyvalue",
				"firstitemsecondkey": "firstitemsecondkeyvalue"
			},
			{
				"seconditemfirstkey": "seconditemfirstkeyvalue",
				"seconditemsecondkey": "seconditemsecondkeyvalue"
			}
		]
	}`)
	confBaseSlice := fftypes.JSONAnyPtr(`[
		{
			"firstitemfirstkey": "firstitemfirstkeyvalue",
			"firstitemsecondkey": "firstitemsecondkeyvalue"
		},
		{
			"seconditemfirstkey": "seconditemfirstkeyvalue",
			"seconditemsecondkey": "seconditemsecondkeyvalue"
		}
	]`)

	viper.Reset()
	viper.Set("base.something", "value4")
	err := MergeConfig([]*fftypes.ConfigRecord{
		{Key: "base", Value: conf1},
		{Key: "base", Value: confNumber},
		{Key: "base.some.plain", Value: conf3},
		{Key: "base", Value: confNestedSlice},
		{Key: "base.slice", Value: confBaseSlice},
	})
	assert.NoError(t, err)

	assert.Equal(t, "value1", viper.Get("base.some.nested.stuff"))
	assert.Equal(t, 15, viper.GetInt("base.some.more.stuff"))
	assert.Equal(t, "value3", viper.Get("base.some.plain"))
	assert.Equal(t, "value4", viper.Get("base.something"))
	assert.Equal(t, "firstitemfirstkeyvalue", viper.Get("base.nestedslice.0.firstitemfirstkey"))
	assert.Equal(t, "seconditemsecondkeyvalue", viper.Get("base.nestedslice.1.seconditemsecondkey"))
	assert.Equal(t, "firstitemfirstkeyvalue", viper.Get("base.slice.0.firstitemfirstkey"))
	assert.Equal(t, "seconditemsecondkeyvalue", viper.Get("base.slice.1.seconditemsecondkey"))

}

func TestMergeConfigBadJSON(t *testing.T) {
	err := MergeConfig([]*fftypes.ConfigRecord{
		{Key: "base", Value: fftypes.JSONAnyPtr(`!json`)},
	})
	assert.Error(t, err)
}

func TestGetConfig(t *testing.T) {
	RootConfigReset()
	conf := GetConfig()
	assert.Equal(t, "info", conf.GetObject("log").GetString("level"))
}

func TestGenerateConfigMarkdown(t *testing.T) {

	key1 := AddRootKey("level1_1.level2_1.level3_1")
	key2 := AddRootKey("level1_1.level2_1.level3_2")
	key3 := AddRootKey("level1_1.level2_2.level3")
	key4 := AddRootKey("level1_2.level2.level3")

	i18n.FFC(language.AmericanEnglish, fmt.Sprintf("config.%s", key1), "Description 1", "Type 1")
	i18n.FFC(language.AmericanEnglish, fmt.Sprintf("config.%s", key2), "Description 2", "Type 2")
	i18n.FFC(language.AmericanEnglish, "config.global.level2_2.level3", "Description 3", "Type 3")
	i18n.FFC(language.AmericanEnglish, fmt.Sprintf("config.%s", key4), "Description 4", "Type 4")

	RootConfigReset(func() {
		viper.SetDefault(string(key1), "val1")
		viper.SetDefault(string(key2), "val2")
		viper.SetDefault(string(key3), "val3")
		viper.SetDefault(string(key4), "val4")
	})

	_, err := GenerateConfigMarkdown(context.Background(), "", []string{
		string(key1), string(key2), string(key3), string(key4),
	})
	assert.NoError(t, err)

}

func TestGenerateConfigMarkdownPanic(t *testing.T) {

	key1 := AddRootKey("bad.key.no.description")

	RootConfigReset()

	assert.Panics(t, func() {
		_, _ = GenerateConfigMarkdown(context.Background(), "", []string{
			string(key1),
		})
	})

}

func TestConfigWatchE2E(t *testing.T) {

	logrus.SetLevel(logrus.DebugLevel)
	tmpDir := t.TempDir()

	// Create the file
	os.WriteFile(fmt.Sprintf("%s/test.yaml", tmpDir), []byte(`{"ut_conf": "one"}`), 0664)

	// Read initial config
	viper.SetConfigType("yaml")
	viper.SetConfigFile(fmt.Sprintf("%s/test.yaml", tmpDir))
	viper.ReadInConfig()
	assert.Equal(t, "one", viper.Get("ut_conf"))

	// Start listener on config file
	fsListenerDone := make(chan struct{})
	fsListenerFired := make(chan bool)
	ctx, cancelCtx := context.WithCancel(context.Background())
	err := WatchConfig(ctx, func() {
		err := viper.ReadInConfig()
		assert.NoError(t, err)
		fsListenerFired <- true
	}, func() {
		close(fsListenerDone)
	})
	assert.NoError(t, err)

	// Delete and rename in another file
	os.Remove(fmt.Sprintf("%s/test.yaml", tmpDir))
	os.WriteFile(fmt.Sprintf("%s/another.yaml", tmpDir), []byte(`{"ut_conf": "two"}`), 0664)
	os.Rename(fmt.Sprintf("%s/another.yaml", tmpDir), fmt.Sprintf("%s/test.yaml", tmpDir))
	<-fsListenerFired
	assert.Equal(t, "two", viper.Get("ut_conf"))

	defer func() {
		cancelCtx()
		if a := recover(); a != nil {
			panic(a)
		}
		<-fsListenerDone
	}()

}

func TestSetEnvPrefix(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := path.Join(tmpDir, "test.yaml")

	// Create the file
	os.WriteFile(configPath, []byte(`{"conf": "one"}`), 0664)

	os.Setenv("TEST_UT_CONF", "two")
	SetEnvPrefix("test")

	RootConfigReset()
	cfg := RootSection("ut")
	cfg.AddKnownKey("conf")

	err := ReadConfig("yaml", configPath)
	require.NoError(t, err)
	assert.Equal(t, "test", viper.GetViper().GetEnvPrefix())

	assert.Equal(t, "two", cfg.GetString("conf"))
}

func TestGetBigInt(t *testing.T) {
	defer RootConfigReset()

	// Test valid decimal
	key1 := AddRootKey("bigint.key1")
	Set(key1, "12345678901234567890")
	result1 := GetBigInt(key1)
	require.NotNil(t, result1)
	expected1 := big.NewInt(0)
	expected1.SetString("12345678901234567890", 10)
	assert.Equal(t, expected1, result1)

	// Test valid hex
	key2 := AddRootKey("bigint.key2")
	Set(key2, "0xFF")
	assert.Equal(t, big.NewInt(255), GetBigInt(key2))

	// Test valid octal
	key3 := AddRootKey("bigint.key3")
	Set(key3, "0777")
	assert.Equal(t, big.NewInt(511), GetBigInt(key3))

	// Test very large value
	key4 := AddRootKey("bigint.key4")
	Set(key4, "999999999999999999999999999999")
	result4 := GetBigInt(key4)
	require.NotNil(t, result4)
	expected4 := big.NewInt(0)
	expected4.SetString("999999999999999999999999999999", 10)
	assert.Equal(t, expected4, result4)

	// Test trimming whitespace
	key5 := AddRootKey("bigint.key5")
	Set(key5, "  999  ")
	assert.Equal(t, big.NewInt(999), GetBigInt(key5))

	// Test zero value
	key6 := AddRootKey("bigint.key6")
	Set(key6, "0")
	assert.Equal(t, big.NewInt(0), GetBigInt(key6))

	// Test negative value
	key7 := AddRootKey("bigint.key7")
	Set(key7, "-123456789")
	assert.Equal(t, big.NewInt(-123456789), GetBigInt(key7))

	// Test empty string returns nil
	key8 := AddRootKey("bigint.key8")
	Set(key8, "")
	assert.Nil(t, GetBigInt(key8))

	// Test whitespace only returns nil
	key9 := AddRootKey("bigint.key9")
	Set(key9, "   ")
	assert.Nil(t, GetBigInt(key9))

	// Test invalid string returns nil
	key10 := AddRootKey("bigint.key10")
	Set(key10, "not a number")
	assert.Nil(t, GetBigInt(key10))

	// Test with config section
	section := RootSection("bigintsection")
	section.AddKnownKey("value")
	section.Set("value", "98765432109876543210")
	resultSection := section.GetBigInt("value")
	require.NotNil(t, resultSection)
	expectedSection := big.NewInt(0)
	expectedSection.SetString("98765432109876543210", 10)
	assert.Equal(t, expectedSection, resultSection)
}
