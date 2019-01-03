/**
 * Copyright 2017 Hortonworks.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/
package com.hortonworks.streamline.streams.layout.beam;

import com.hortonworks.streamline.common.exception.*;
import com.hortonworks.streamline.streams.*;
import com.hortonworks.streamline.streams.layout.*;
import org.apache.beam.sdk.*;
import org.apache.beam.sdk.values.*;
import sun.reflect.generics.reflectiveObjects.*;

import java.util.*;

/**
 * Abstract implementation of FluxComponent interface. Child classes just
 * need to implement generateComponent method using conf and update referenced
 * components and component variables
 */
public abstract class AbstractBeamComponent implements BeamComponent {
    protected final UUID UUID_FOR_COMPONENTS = UUID.randomUUID();
    // conf is the map representing the configuration parameters for this
    // storm component picked by the user. For eg kafka component will have
    // zkUrl, topic, etc.
    protected Map<String, Object> conf;
    protected boolean isGenerated;
    protected transient Pipeline pipeline;
    protected Map<String, Object> component = new LinkedHashMap<>();

    @Override
    public void withCatalogRootUrl(String catalogRootUrl) {
        throw new NotImplementedException();
//        this.catalogRestClient = new CatalogRestClient(catalogRootUrl);
    }

    @Override
    public void withConfig(Map<String, Object> conf, Pipeline pipeline) {
        this.conf = conf;
        this.pipeline = pipeline;
    }

    @Override
    public abstract PCollection<StreamlineEvent> getOutputCollection();

    @Override
    public abstract void unionInputCollection(PCollection<StreamlineEvent> collection);

    @Override
    public void validateConfig()
            throws ComponentConfigException {
        String[] fieldNames = {TopologyLayoutConstants.JSON_KEY_PARALLELISM};
        Long[] mins = {1L};
        Long[] maxes = {Long.MAX_VALUE};
        this.validateLongFields(fieldNames, false, mins, maxes);
    }

    protected List<Map<String, Object>> getConfigMethodsYaml(String[] configMethodNames, String[] configKeys) {
        List<Map<String, Object>> configMethods = new ArrayList<>();
        List<String> nonNullConfigMethodNames = new ArrayList<>();
        List<Object[]> values = new ArrayList<Object[]>(configKeys == null ? 0 : configKeys.length);

        if ((configMethodNames != null) && (configKeys != null) &&
                (configMethodNames.length == configKeys.length) && (configKeys.length > 0)) {
            for (int i = 0; i < configKeys.length; ++i) {
                if (conf.get(configKeys[i]) != null) {
                    nonNullConfigMethodNames.add(configMethodNames[i]);
                    Object args = conf.get(configKeys[i]);
                    if (args.getClass().isArray()) {
                        values.add((Object[]) args);
                    } else {
                        if (args != Args.NONE) {
                            values.add(new Object[]{args});
                        }
                    }
                }
            }
            configMethods = getConfigMethodsYaml2(nonNullConfigMethodNames.toArray(new String[0]), make2dArray(values));
        }
        return configMethods;
    }

    private Object[][] make2dArray(List<Object[]> values) {
        Object[][] result = new Object[values.size()][];
        for (int i = 0; i < values.size(); i++) {
            result[i] = values.get(i);
        }
        return result;
    }

    protected List<Map<String, Object>> getConfigMethodsYaml2(String[] configMethodNames, Object[][] values) {
        List<Map<String, Object>> configMethods = new ArrayList<>();
        if ((configMethodNames != null) && (values != null) &&
                (configMethodNames.length == values.length) && (values.length > 0)) {
            for (int i = 0; i < values.length; ++i) {
                Map<String, Object> configMethod = new LinkedHashMap<>();
                configMethod.put(TopologyLayoutConstants.YAML_KEY_NAME, configMethodNames[i]);
                List<Object> methodArgs = new ArrayList<>();
                Object[] value = values[i];
                for (int j = 0; j < value.length; j++) {
                    if (value[j] != Args.NONE) {
                        methodArgs.add(value[j]);
                    }
                }

                configMethod.put(TopologyLayoutConstants.YAML_KEY_ARGS, methodArgs);
                configMethods.add(configMethod);
            }
        }
        return configMethods;
    }

    // validate boolean fields based on if they are required or not. Meant to
    // be called from base classes that need to validate
    protected void validateBooleanFields(String[] fieldNames, boolean areRequiredFields)
            throws ComponentConfigException {
        this.validateBooleanFields(fieldNames, areRequiredFields, conf);
    }

    // Overloaded version of above method since we need it for NotificationBolt and perhaps other components in future
    protected void validateBooleanFields(String[] fieldNames, boolean areRequiredFields, Map<String, Object> conf)
            throws ComponentConfigException {
        for (String fieldName : fieldNames) {
            Object value = conf.get(fieldName);
            boolean isValid = true;
            if (areRequiredFields) {
                // validate no matter what for required fields
                if (!ConfigFieldValidation.isBoolean(value)) {
                    isValid = false;
                }
            } else {
                // for optional fields validate only if user updated the
                // default value which means UI put it in json
                if ((value != null) && !ConfigFieldValidation.isBoolean(value)) {
                    isValid = false;
                }
            }
            if (!isValid) {
                throw new ComponentConfigException(String.format(TopologyLayoutConstants.ERR_MSG_MISSING_INVALID_CONFIG, fieldName));
            }
        }
    }

    // validate string fields based on if they are required or not. Meant to
    // be called from base classes that need to validate
    protected void validateStringFields(String[] fieldNames, boolean areRequiredFields)
            throws ComponentConfigException {
        this.validateStringFields(fieldNames, areRequiredFields, conf);
    }

    // Overloaded version of above method since we need it for NotificationBolt and perhaps other components in future
    protected void validateStringFields(String[] fieldNames, boolean areRequiredFields, Map<String, Object> conf)
            throws ComponentConfigException {
        for (String fieldName : fieldNames) {
            Object value = conf.get(fieldName);
            boolean isValid = true;
            if (areRequiredFields) {
                // validate no matter what for required fields
                if (!ConfigFieldValidation.isStringAndNotEmpty(value)) {
                    isValid = false;
                }
            } else {
                // for optional fields validate only if user updated the
                // default value which means UI put it in json
                if ((value != null) && !ConfigFieldValidation.isStringAndNotEmpty(value)) {
                    isValid = false;
                }
            }
            if (!isValid) {
                throw new ComponentConfigException(String.format(TopologyLayoutConstants.ERR_MSG_MISSING_INVALID_CONFIG, fieldName));
            }
        }
    }

    // validate byte fields based on if they are required or not and their
    // valid range. Meant to // be called from base classes that need to validate
    protected void validateByteFields(String[] fieldNames, boolean
            areRequiredFields, Byte[] mins, Byte[] maxes)
            throws
            ComponentConfigException {
        if ((fieldNames == null) || (fieldNames.length != mins.length) ||
                (fieldNames.length != maxes.length)) {
            return;
        }
        for (int i = 0; i < fieldNames.length; ++i) {
            String fieldName = fieldNames[i];
            Object value = conf.get(fieldName);
            Byte min = mins[i];
            Byte max = maxes[i];
            boolean isValid = true;
            if (areRequiredFields) {
                // validate no matter what for required fields
                if (!ConfigFieldValidation.isByteAndInRange(value, min, max)) {
                    isValid = false;
                }
            } else {
                // for optional fields validate only if user updated the
                // default value which means UI put it in json
                if ((value != null) && !ConfigFieldValidation.isByteAndInRange(value, min, max)) {
                    isValid = false;
                }
            }
            if (!isValid) {
                throw new ComponentConfigException(String.format(TopologyLayoutConstants.ERR_MSG_MISSING_INVALID_CONFIG, fieldName));
            }
        }
    }

    // validate short fields based on if they are required or not and their
    // valid range. Meant to // be called from base classes that need to validate
    protected void validateShortFields(String[] fieldNames, boolean
            areRequiredFields, Short[] mins, Short[] maxes)
            throws
            ComponentConfigException {
        if ((fieldNames == null) || (fieldNames.length != mins.length) ||
                (fieldNames.length != maxes.length)) {
            return;
        }
        for (int i = 0; i < fieldNames.length; ++i) {
            String fieldName = fieldNames[i];
            Object value = conf.get(fieldName);
            Short min = mins[i];
            Short max = maxes[i];
            boolean isValid = true;
            if (areRequiredFields) {
                // validate no matter what for required fields
                if (!ConfigFieldValidation.isShortAndInRange(value, min, max)) {
                    isValid = false;
                }
            } else {
                // for optional fields validate only if user updated the
                // default value which means UI put it in json
                if ((value != null) && !ConfigFieldValidation.isShortAndInRange(value, min, max)) {
                    isValid = false;
                }
            }
            if (!isValid) {
                throw new ComponentConfigException(String.format(TopologyLayoutConstants.ERR_MSG_MISSING_INVALID_CONFIG, fieldName));
            }
        }
    }

    // validate integer fields based on if they are required or not and their
    // valid range. Meant to // be called from base classes that need to validate
    protected void validateIntegerFields(String[] fieldNames, boolean areRequiredFields, Integer[] mins, Integer[] maxes)
            throws ComponentConfigException {
        this.validateIntegerFields(fieldNames, areRequiredFields, mins, maxes, conf);
    }

    // Overloaded version of above method since we need it for NotificationBolt and perhaps other components in future
    protected void validateIntegerFields(String[] fieldNames, boolean areRequiredFields, Integer[] mins, Integer[] maxes, Map<String, Object> conf)
            throws
            ComponentConfigException {
        if ((fieldNames == null) || (fieldNames.length != mins.length) ||
                (fieldNames.length != maxes.length)) {
            return;
        }
        for (int i = 0; i < fieldNames.length; ++i) {
            String fieldName = fieldNames[i];
            Object value = conf.get(fieldName);
            Integer min = mins[i];
            Integer max = maxes[i];
            boolean isValid = true;
            if (areRequiredFields) {
                // validate no matter what for required fields
                if (!ConfigFieldValidation.isIntAndInRange(value, min, max)) {
                    isValid = false;
                }
            } else {
                // for optional fields validate only if user updated the
                // default value which means UI put it in json
                if ((value != null) && !ConfigFieldValidation.isIntAndInRange(value, min, max)) {
                    isValid = false;
                }
            }
            if (!isValid) {
                throw new ComponentConfigException(String.format(TopologyLayoutConstants.ERR_MSG_MISSING_INVALID_CONFIG, fieldName));
            }
        }
    }

    // validate long fields based on if they are required or not and their
    // valid range. Meant to // be called from base classes that need to validate
    protected void validateLongFields(String[] fieldNames, boolean
            areRequiredFields, Long[] mins, Long[] maxes)
            throws
            ComponentConfigException {
        if ((fieldNames == null) || (fieldNames.length != mins.length) ||
                (fieldNames.length != maxes.length)) {
            return;
        }
        for (int i = 0; i < fieldNames.length; ++i) {
            String fieldName = fieldNames[i];
            Object value = conf.get(fieldName);
            Long min = mins[i];
            Long max = maxes[i];
            boolean isValid = true;
            if (areRequiredFields) {
                // validate no matter what for required fields
                if (!ConfigFieldValidation.isLongAndInRange(value, min, max)) {
                    isValid = false;
                }
            } else {
                // for optional fields validate only if user updated the
                // default value which means UI put it in json
                if ((value != null) && !ConfigFieldValidation.isLongAndInRange(value, min, max)) {
                    isValid = false;
                }
            }
            if (!isValid) {
                throw new ComponentConfigException(String.format(TopologyLayoutConstants.ERR_MSG_MISSING_INVALID_CONFIG, fieldName));
            }
        }
    }

    protected void validateFloatOrDoubleFields(String[] fieldNames, boolean
            areRequiredFields)
            throws ComponentConfigException {
        for (String fieldName : fieldNames) {
            Object value = conf.get(fieldName);
            boolean isValid = true;
            if (areRequiredFields) {
                // validate no matter what for required fields
                if (!ConfigFieldValidation.isFloatOrDouble(value)) {
                    isValid = false;
                }
            } else {
                // for optional fields validate only if user updated the
                // default value which means UI put it in json
                if ((value != null) && !ConfigFieldValidation.isFloatOrDouble(value)) {
                    isValid = false;
                }
            }
            if (!isValid) {
                throw new ComponentConfigException(String.format(TopologyLayoutConstants.ERR_MSG_MISSING_INVALID_CONFIG, fieldName));
            }
        }
    }

    protected List<Object> makeConstructorArgs(String... keys) {
        List<Object> args = new ArrayList<>();
        for (String key : keys) {
            addArg(args, key);
        }
        return args;
    }

    protected <T> void addArg(final List<? super T> args, T key) {
        T value = (T) conf.get(key);
        if (value == null) {
            throw new IllegalArgumentException("Value for key '" + key + "' not found in config");
        }
        args.add(value);
    }

    protected <T> void addArg(final List<? super T> args, String key, T defaultValue) {
        T value = (T) conf.get(key);
        if (value != null) {
            args.add(value);
        } else {
            args.add(defaultValue);
        }
    }

    protected void addArg(List<Object> args, Map<String, String> ref) {
        args.add(ref);
    }

    protected enum Args {
        NONE
    }
}