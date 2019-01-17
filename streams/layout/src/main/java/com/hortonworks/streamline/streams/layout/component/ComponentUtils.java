package com.hortonworks.streamline.streams.layout.component;

import com.hortonworks.streamline.common.exception.ComponentConfigException;
import com.hortonworks.streamline.streams.layout.ConfigFieldValidation;
import com.hortonworks.streamline.streams.layout.TopologyLayoutConstants;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by Satendra Sahu on 1/9/19
 */
public class ComponentUtils {



  // Overloaded version of above method since we need it for NotificationBolt and perhaps other components in future
  public void validateBooleanFields(String[] fieldNames, boolean areRequiredFields,
      Map<String, Object> conf) throws ComponentConfigException {
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
        throw new ComponentConfigException(
            String.format(TopologyLayoutConstants.ERR_MSG_MISSING_INVALID_CONFIG, fieldName));
      }
    }
  }


  // Overloaded version of above method since we need it for NotificationBolt and perhaps other components in future
  public void validateStringFields(String[] fieldNames, boolean areRequiredFields,
      Map<String, Object> conf) throws ComponentConfigException {
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
        throw new ComponentConfigException(
            String.format(TopologyLayoutConstants.ERR_MSG_MISSING_INVALID_CONFIG, fieldName));
      }
    }
  }

  // validate byte fields based on if they are required or not and their
  // valid range. Meant to // be called from base classes that need to validate
  public void validateByteFields(String[] fieldNames, boolean
      areRequiredFields, Byte[] mins, Byte[] maxes, Map<String, Object> conf) throws
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
        throw new ComponentConfigException(
            String.format(TopologyLayoutConstants.ERR_MSG_MISSING_INVALID_CONFIG, fieldName));
      }
    }
  }

  // validate short fields based on if they are required or not and their
  // valid range. Meant to // be called from base classes that need to validate
  public void validateShortFields(String[] fieldNames, boolean
      areRequiredFields, Short[] mins, Short[] maxes, Map<String, Object> conf) throws
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
        throw new ComponentConfigException(
            String.format(TopologyLayoutConstants.ERR_MSG_MISSING_INVALID_CONFIG, fieldName));
      }
    }
  }

  // Overloaded version of above method since we need it for NotificationBolt and perhaps other components in future
  public void validateIntegerFields(String[] fieldNames, boolean areRequiredFields,
      Integer[] mins, Integer[] maxes, Map<String, Object> conf) throws
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
        throw new ComponentConfigException(
            String.format(TopologyLayoutConstants.ERR_MSG_MISSING_INVALID_CONFIG, fieldName));
      }
    }
  }

  // validate long fields based on if they are required or not and their
  // valid range. Meant to // be called from base classes that need to validate
  public void validateLongFields(String[] fieldNames, boolean
      areRequiredFields, Long[] mins, Long[] maxes, Map<String, Object> conf) throws
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
        throw new ComponentConfigException(
            String.format(TopologyLayoutConstants.ERR_MSG_MISSING_INVALID_CONFIG, fieldName));
      }
    }
  }

  public void validateFloatOrDoubleFields(String[] fieldNames, boolean
      areRequiredFields, Map<String, Object> conf) throws ComponentConfigException {
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
        throw new ComponentConfigException(
            String.format(TopologyLayoutConstants.ERR_MSG_MISSING_INVALID_CONFIG, fieldName));
      }
    }
  }
}
