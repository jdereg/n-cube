package com.cedarsoftware.visualizer

import com.cedarsoftware.ncube.ApplicationID
import com.cedarsoftware.ncube.Axis
import com.cedarsoftware.ncube.Column
import com.cedarsoftware.ncube.NCube
import com.cedarsoftware.ncube.NCubeRuntimeClient
import com.cedarsoftware.ncube.exception.InvalidCoordinateException
import com.cedarsoftware.util.CaseInsensitiveSet
import com.cedarsoftware.util.CompactCILinkedMap
import groovy.transform.CompileStatic

import java.util.regex.Matcher
import java.util.regex.Pattern

import static com.cedarsoftware.ncube.NCubeAppContext.getNcubeRuntime
import static com.cedarsoftware.util.StringUtilities.isEmpty

/**
 * Provides helper methods to load fields and traits for an rpm class.
 * The methods are are copied from Dynamis unless otherwise is indicated. Some methods are slightly altered.
 * Do find on 'COPIED' to find code copied from Dynamis.
 * Do find on 'ORIGINAL' to find code not copied from Dynamis.
 */

@CompileStatic
class RpmVisualizerHelper extends VisualizerHelper
{
	/**
	 * ORIGINAL: Not copied from Dynamis
	 */
	public static final String FIELD_AXIS = "field"
	public static final String TRAIT_AXIS = "trait"
	public static final String ENUM_NAME_AXIS = "name"
	public static final String RPM_CLASS = "rpm.class"
	public static final String RPM_ENUM = "rpm.enum"
	public static final String R_EXTENDS = 'r:extends'
	public static final String R_EXISTS = 'r:exists'
	public static final String R_RPM_TYPE = 'r:rpmType'
	public static final String R_SCOPED_NAME = 'r:scopedName'
	public static final String R_DECLARED = 'r:declared'
	public static final String R_SINCE = 'r:since'
	public static final String R_OBSOLETE = 'r:obsolete'
	public static final String V_ENUM = 'v:enum'
	public static final String V_MIN = 'v:min'
	public static final String V_MAX = 'v:max'
	private static final String NOT_DEFINED = '#NOT_DEFINED'
	public static final String CLASS_TRAITS = 'CLASS_TRAITS'
	public static final String SYSTEM_SCOPE_KEY_PREFIX = "_"
	public static final String EFFECTIVE_VERSION_SCOPE_KEY = SYSTEM_SCOPE_KEY_PREFIX + "effectiveVersion"
	public static final List MINIMAL_TRAITS = [R_RPM_TYPE, R_SCOPED_NAME, R_EXTENDS, R_EXISTS, R_DECLARED, R_SINCE, R_OBSOLETE, V_ENUM, V_MIN, V_MAX]
	private static final String EXISTS_TRAIT_CONTAINS_NULL_VALUE = " may not contain a value of null. If there is a value, it must be true or false. "
	private static ApplicationID appId
	private static boolean loadAllTraits

	RpmVisualizerHelper(NCubeRuntimeClient runtimeClient, ApplicationID appId)
	{
		super(runtimeClient, appId)
	}

	/**
	 * COPIED: From Dynamis 5.2.0  (except for slight modifications)
	 */
	public static
	final Pattern PATTERN_CLASS_NAME = Pattern.compile('^(?:[a-z][a-z0-9_]*)(?:\\.[a-z][a-z0-9_]*)*$', Pattern.CASE_INSENSITIVE)

	public static
	final Pattern PATTERN_CLASS_EXTENDS_TRAIT = Pattern.compile('[^,\\s][^\\,]*[^,\\s]*', Pattern.CASE_INSENSITIVE)

	public static
	final Pattern PATTERN_FIELD_EXTENDS_TRAIT = Pattern.compile('^\\s*((?:[a-z][a-z0-9_]*)(?:\\.[a-z][a-z0-9_]*)*)\\s*(?:[\\[]\\s*([a-z0-9_]+?)\\s*[\\]])?\\s*$', Pattern.CASE_INSENSITIVE)

	/**
	 * COPIED: From Dynamis 5.2.0  (except for slight modifications)
	 */
	boolean isPrimitive(String type){
		for (PRIMITIVE_TYPE pt : PRIMITIVE_TYPE.values()) {
			if (pt.getClassType().getSimpleName().equalsIgnoreCase(type)) {
				return true
            }
		}
		return false
    }

	/**
	 * COPIED: From Dynamis 5.2.0  (except for slight modifications)
	 * pulls all of the fields and associated traits from nCube that will be used to create the RpmClass/RpmEnum instance
	 */
	void loadRpmClassFields(ApplicationID applicationId, String cubeType, String cubeName, Map<String, Object> scope, Map<String, Map<String, Object>> traitMaps, boolean loadAllTraits, Map<String, Object> output)
	{
		this.appId = applicationId
		this.loadAllTraits = loadAllTraits

		LinkedList<String> classesToProcess = new LinkedList<String>()
        Set<String> visited = new LinkedHashSet<String>()

        // loop through class hierarchy until all classes in the r:extends chain have been handled
		boolean isOriginalClass = true
        classesToProcess.add(cubeName)
        while (!classesToProcess.isEmpty())
		{
			String className = classesToProcess.pop()

            // don't allow cycles
			if (visited.contains(className))
			{
				continue
            }
			visited.add(className)

            try
			{
				loadFieldTraitsForClass(cubeType, className, scope, traitMaps, classesToProcess, output)
                if(isOriginalClass)
				{
					for(Map.Entry<String,Map<String,Object>> entry : traitMaps.entrySet())
					{
						if(!CLASS_TRAITS.equals(entry.getKey()))
						{
							entry.getValue().put(R_DECLARED, true)
                        }
					}
				}
				isOriginalClass = false
            }
			catch (Exception e)
			{
				handleException(cubeType,visited,className,e)
            }
		} // end class stack
	}

	/**
	 * COPIED: From Dynamis 5.2.0  (except for slight modifications)
	 * Populates the field traits for the class or enum
	 */
	private void loadFieldTraitsForClass(String cubeType, String className, Map<String, Object> scope, Map<String,Map<String,Object>> fieldAndTraits, LinkedList<String> classesToProcess, Map<String, Object> output)
	{
		String axisName = RPM_ENUM.equals(cubeType) ? ENUM_NAME_AXIS : FIELD_AXIS

        NCube classCube = findClassCube(cubeType, scope, className, output)
        if (classCube == null)
		{
			String classType = cubeType==RPM_CLASS ? "RpmClass" : "RpmEnum"
            throw new IllegalArgumentException(classType + " definition not found for identifier='" + className + "'")
        }

		// populate initial fields
		populateAllFieldsFromAxis(fieldAndTraits, axisName, classCube)

        // wildcard the fieldAxis using r:exists
		populateExistsTrait(className, axisName, scope, fieldAndTraits, classCube, output)

        // determine traits to fetch, except for r:exists already fetched
		List<String> traitNames = getTraitNamesForCube(classCube, (String) scope.get(EFFECTIVE_VERSION_SCOPE_KEY))
        traitNames.remove(R_EXISTS)

        // pull traits for existing fields
		Axis fieldAxis = classCube.getAxis(axisName)
        for(String fieldName : fieldAndTraits.keySet()) {
			Map<String,Object> fieldTraits = fieldAndTraits.get(fieldName)

            // short circuit the fields that don't exist
			Boolean exist = (Boolean) fieldTraits.get(R_EXISTS)
            if (Boolean.FALSE.equals(exist)) {
				continue
			}
			else if (fieldAxis.findColumn(fieldName)==null) {
				continue
			}

			// gather traits for current field that haven't already been populated
			Map<String, Object> coord = new LinkedHashMap<>(scope)
			coord.put(axisName,fieldName)
			loadTraitsForField(classCube, traitNames, fieldTraits, coord, output)

            // eliminate scoped fields
			if (!isFieldValidSince(fieldTraits,(String) scope.get(EFFECTIVE_VERSION_SCOPE_KEY))) {
				fieldTraits.put(R_EXISTS,false)
            }
			if (!isFieldValidObsolete(fieldTraits,(String) scope.get(EFFECTIVE_VERSION_SCOPE_KEY))) {
				fieldTraits.put(R_EXISTS,false)
            }

			// check extends value
			if (traitNames.contains(R_EXTENDS)) {
				coord.put(TRAIT_AXIS, R_EXTENDS)
                Object extendsValue = classCube.getCell(coord, output, NOT_DEFINED)
                if (extendsValue!=null && hasValue(extendsValue))
				{
					if (!fieldTraits.containsKey(R_EXTENDS)) {
						fieldTraits.put(R_EXTENDS,extendsValue.toString())
                    }

					if (CLASS_TRAITS.equals(fieldName)) {
						processClassMixins(className, extendsValue.toString(), classesToProcess)
                    }
					else {
						processMasterDefinition(className, fieldName, extendsValue.toString(), cubeType, axisName, scope, fieldTraits, output)
                    }
				}
			}
		}
	}


	/**
	 * COPIED: From Dynamis 5.2.0  (except for slight modifications)
	 * applies the master definition specified in r:extends to the current field traits
	 */
	private void processMasterDefinition(String className, String fieldName, String masterDefinition,
										 String cubeType, String axisName, Map<String, Object> scope, Map<String, Object> traits, Map<String, Object> output)
	{
		String classType = cubeType==RPM_CLASS ? "RpmClass" : "RpmEnum"
        LinkedList<String> defsToProcess = new LinkedList<String>()
        Set<String> visited = new HashSet<>()	// list of visited definitions pulled from traits using class_name or class_name[field_name] format
		Set<String> fqVisited = new HashSet<>()	// list of visited definitions which have been fully qualified, e.g. class_name[field_name]
		final String exceptionFormat = "%s for field='%s' of %s='%s': definition(s)='%s'"

        defsToProcess.add(masterDefinition)
        while (!defsToProcess.empty) {
			String fieldToMerge = defsToProcess.pop()
            visited.add(fieldToMerge)

            Matcher m = PATTERN_FIELD_EXTENDS_TRAIT.matcher(fieldToMerge)
            if (!m.matches())
			{
				throw new IllegalArgumentException(String.format(exceptionFormat, "Invalid master definition format used", fieldName,
						classType, className, Arrays.toString(visited.toArray())))
            }

			// determine the master definition to use
			String masterClass = m.group(1)
            String masterField = isEmpty(m.group(2)) ? fieldName : m.group(2)
            String fqMasterDef = masterClass + "[" + masterField + "]"	// fully qualified master definition

			// make sure class hasn't already been processed
			if (fqVisited.contains(fqMasterDef))
			{
				continue
            }
			fqVisited.add(fqMasterDef)

            // make sure the class definition exists
			NCube masterCube = findClassCube(cubeType, scope, masterClass, output)
            if (masterCube == null)
			{
				throw new IllegalArgumentException(String.format(exceptionFormat, "Class in master definition not found", fieldName,
						classType, className, Arrays.toString(visited.toArray())))
            }

			// validate the field name
			boolean validField = masterCube.getAxis(axisName).findColumn(masterField) != null
            if (!validField) {
				throw new IllegalArgumentException(String.format(exceptionFormat, "Field in master definition not found", fieldName,
						classType, className, Arrays.toString(visited.toArray())))
            }

			Map<String,Object> coord = new CompactCILinkedMap<>(scope)
            coord.put(axisName,masterField)
            List<String> traitNames = getTraitNamesForCube(masterCube, (String) scope.get(EFFECTIVE_VERSION_SCOPE_KEY))
            loadTraitsForField(masterCube, traitNames, traits, coord, output)

            if (traits.containsKey(R_EXISTS) && traits.get(R_EXISTS) ==  null){
				throw new IllegalArgumentException(String.format(exceptionFormat, R_EXISTS + EXISTS_TRAIT_CONTAINS_NULL_VALUE, fieldName,
						classType, className, Arrays.toString(visited.toArray())))
            }

			// check for extended definitions
			if (traitNames.contains(R_EXTENDS)) {
				coord.put(TRAIT_AXIS, R_EXTENDS)
                String extension = (String) masterCube.getCell(coord, output, NOT_DEFINED)
                if (hasValue(extension) && !isEmpty(extension)) {
					defsToProcess.add(extension)
                }
			}
		} // end while
	}

	/**
	 * COPIED: From Dynamis 5.2.0  (except for slight modifications)
	 * Returns the nCube for the specified class (or enum)
	 */
	private static NCube findClassCube(String cubeType, Map<String, Object> scope, String className, Map<String, Object> output) {
		if (className==null || !PATTERN_CLASS_NAME.matcher(className).matches())
		{
			throw new IllegalArgumentException("Invalid class identifier [" + className + "] was specified for " + cubeType)
        }

		NCube ncube = ncubeRuntime.getCube(appId, cubeType + "." + className)
        if (ncube==null)
		{
			return null
        }

		Set<String> requiredScope = getRequiredScope(ncube, scope, output)
        if (RPM_ENUM.equals(cubeType))
		{
			requiredScope.remove("name")
        }
		ensureEnoughScopeProvided(cubeType, className, scope, requiredScope)
        return ncube
    }


	/**
	 * COPIED: From Dynamis 5.2.0  (except for slight modifications)
	 * parses the value of the r:extends trait and adds all mixins to the list of classes to process
	 */
	private static void processClassMixins(String className, String mixins, LinkedList<String> classesToProcess) {
		if (classesToProcess == null)
		{
			return
        }

		Matcher matcher = PATTERN_CLASS_EXTENDS_TRAIT.matcher(mixins)
        if (isEmpty(mixins) || !matcher.find())
		{
			throw new IllegalArgumentException("Invalid mixin format specified for class='${className}': mixin='${mixins}'")
        }

		for (; ;) { // infinite for
			String mixinName = matcher.group(0)
            if (!isEmpty(mixinName))
			{
				classesToProcess.addFirst(mixinName.trim())
            }

			if (!matcher.find())
			{ //condition to break, opposite to while
				break
			}
		}
	}

	/**
	 * COPIED: From Dynamis 5.2.0  (except for slight modifications)
	 */
	private enum PRIMITIVE_TYPE {
		BOOLEAN (Boolean.class), LONG(Long.class), DOUBLE(Double.class), BIG_DECIMAL(BigDecimal.class), STRING(String.class), DATE(Date.class);

		private Class<?> classType

        private PRIMITIVE_TYPE(Class<?> classType) {
			this.classType = classType
        }

        Class<?> getClassType() {
			return this.classType
        }

        static PRIMITIVE_TYPE fromName(String typeName) {
			for (PRIMITIVE_TYPE type : values()) {
				if (type.toString().equalsIgnoreCase(typeName) || type.getClassType().getSimpleName().equalsIgnoreCase(typeName)) {
					return type
                }
			}

			throw new IllegalArgumentException("Unknown primitive type specified: " + typeName)
        }
	}

	/**
	 * COPIED: From Dynamis 5.2.0  (except for slight modifications)
	 * Throws RpmException which includes list of classes processed and cause
	 */
	private static void handleException(String cubeType, Set<String> visited, String className, Exception e) {
		// to help with debugging issues related to classes using mixins, dump the list of classes processed thus far
		StringBuilder msg = new StringBuilder()
        msg.append("Failed to load " + (cubeType==RPM_CLASS ? "RpmClass" : "RpmEnum") + "='")
        msg.append(className)
        msg.append("'")
        if (visited.size()>1)
		{
			msg.append(", classes processed=")
            msg.append(Arrays.toString(visited.toArray()))
        }
		throw new Exception( msg.toString(), e)
    }

	/**
	 * COPIED: From Dynamis 5.2.0  (except for slight modifications)
	 * Load trait values for a given field into the fieldTraits map, ignoring traits already loaded
	 */
	private void loadTraitsForField(NCube classCube, List<String> traitNames, Map<String, Object> fieldTraits, Map<String, Object> coord, Map<String, Object> output) {
		for (String traitName : traitNames) {
			if (fieldTraits.containsKey(traitName) || R_EXTENDS.equals(traitName)) {
				continue
            }

			coord.put(TRAIT_AXIS,traitName)
            Object val = classCube.getCell(coord, output, NOT_DEFINED)
            if (hasValue(val)) {
				fieldTraits.put(traitName, val)
            }
		}
	}

	/**
	 * COPIED: From Dynamis 5.2.0  (except for slight modifications)
	 * Determines initial list of fields by extracting column values from axis
	 */
	private static void populateAllFieldsFromAxis(Map<String, Map<String, Object>> fieldAndTraits, String axisName, NCube classCube) {
		for (Column c:classCube.getAxis(axisName).getColumns()) {
			String fieldName = c.getValueThatMatches().toString()

            Map<String,Object> fieldTraits = fieldAndTraits.get(fieldName)
            if (fieldTraits==null) {
				fieldTraits = new LinkedHashMap<>()
                fieldAndTraits.put(fieldName,fieldTraits)
            }
		}
	}

	/**
	 * COPIED: From Dynamis 5.2.0  (except for slight modifications)
	 */
	private boolean isFieldValidSince(Map<String, Object> traits, String sourceVersion) {
		if (!traits.containsKey(R_SINCE)) {
			return true
        }

		Object sinceVersionString = traits.get(R_SINCE)
        ComparableVersion sinceVersion = new ComparableVersion(sinceVersionString.toString())
        ComparableVersion version = new ComparableVersion(sourceVersion)
        return version.compareTo(sinceVersion) >= 0
    }


	/**
	 * COPIED: From Dynamis 5.2.0  (except for slight modifications)
	 */
	private boolean isFieldValidObsolete(Map<String, Object> traits, String sourceVersion) {
		if (!traits.containsKey(R_OBSOLETE)) {
			return true
        }

		Object obsoleteVersionString = traits.get(R_OBSOLETE)
        ComparableVersion obsoleteVersion = new ComparableVersion(obsoleteVersionString.toString())
        ComparableVersion version = new ComparableVersion(sourceVersion)
        return version.compareTo(obsoleteVersion) < 0
    }


	/**
	 * COPIED: From Dynamis 5.2.0  (except for slight modifications)
	 * Return List of Strings, containing names of trait columns defined on the NCube specified
	 */
	private static List<String> getTraitNamesForCube(NCube classCube, String sourceVersion) {
		Axis traitAxis = classCube.getAxis(TRAIT_AXIS)
        List<String> traitNames = new ArrayList<>()
        for (Column c:traitAxis.getColumns()) {
			String traitName = c.getValue().toString()
            if (loadAllTraits || MINIMAL_TRAITS.contains(traitName)){
				traitNames.add(traitName)
            }
		}
		return traitNames
    }

	/**
	 * COPIED: From Dynamis 5.2.0  (except for slight modifications)
	 * Get the 'proper' requiredScope from NCube.  In addition to getting the scope
	 * keys (Strings), the associated Set is all the values used for the given scope
	 * key, akin to all enums in an enum list.
	 */
	private static Set<String> getRequiredScope(NCube ncube, Map<String, Object> scope, Map<String, Object> output)
	{
		Set<String> requiredScope = new CaseInsensitiveSet<String>(ncube.getRequiredScope(scope, output))

        // Although 'field' and 'trait' are axes on the ncube defining the class/enum/rel, they are system
		// scope, not business scope.
		requiredScope.remove(FIELD_AXIS)
        requiredScope.remove(TRAIT_AXIS)
        return requiredScope
    }

	/**
	 * MODIFIED: From Dynamis 5.2.0. Modified to throw InvalidCoordinateException
	 * Ensure that enough scope is provided.  This will check that the original scope key set
	 * has all the keys required to reach all cells in the defining ncube.
	 */
	private static void ensureEnoughScopeProvided(String cubeType, String className, Map<String, Object> scope, Set<String> requiredScope)
	{
		if (!scope.keySet().containsAll(requiredScope))
		{
			Set<String> missingScope = new CaseInsensitiveSet<String>(requiredScope)
            Set scopeKeySet = scope.keySet()
			for (String scopeKey : scopeKeySet)
			{
				missingScope.remove(scopeKey)
            }
			String cubeName = "${cubeType}.${className}"
			throw new InvalidCoordinateException("Not enough scope was provided to create class/enum/rel: ${className}, missing scope keys: ${missingScope}", cubeName, scopeKeySet, requiredScope)
		}
	}

	/**
	 * COPIED: From Dynamis 5.2.0  (except for slight modifications)
	 * Bulk loads value of r:exists for all fields defined
	 */
	private void populateExistsTrait(String className, String axisName, Map<String, Object> scope, Map<String, Map<String, Object>> fieldAndTraits, NCube classCube, Map output) {
		Axis traitAxis = classCube.getAxis(TRAIT_AXIS)
        if (traitAxis==null || traitAxis.findColumn(R_EXISTS)==null) {
			return
        }

		Map<String, Object> coord = new LinkedHashMap<>(scope)
        coord.put(TRAIT_AXIS, R_EXISTS)
        for (Column c : classCube.getAxis(axisName).getColumns()) {
			String fieldName = (String) c.getValue()

            Map<String, Object> fieldTraits = fieldAndTraits.get(fieldName)
            if (!fieldTraits.containsKey(R_EXISTS)) {
				coord.put(axisName,fieldName)
                Boolean exists = getExistsValue(fieldName, className, classCube.getCell(coord, output, NOT_DEFINED))
                if (exists!=null) {
					fieldTraits.put(R_EXISTS, exists)
                }
			}
		}
	}

	/**
	 * COPIED: From Dynamis 5.2.0  (except for slight modifications)
	 * Returns the value of the r:exists trait, or null if not set
	 * @return the boolean value of r:exists, if it exists; otherwise, null
	 */
	private static Boolean getExistsValue(String fieldName, String className, Object exists) {
		if (CLASS_TRAITS.equals(fieldName)) {
			return true
        }

		if (exists==null)
		{
			throw new IllegalStateException(R_EXISTS + EXISTS_TRAIT_CONTAINS_NULL_VALUE + "field: "+ fieldName + ", rpmClass: "+ className)
        }

		if (!hasValue(exists)) {
			return null
        }

		if (exists instanceof String)
		{
			exists = Boolean.valueOf((String)exists)
        }
		else if(!(exists instanceof Boolean))
		{
			throw new IllegalStateException(R_EXISTS + " must be boolean or string. field: "+ fieldName + ", rpmClass: "+ className)
        }

		return (Boolean)exists
    }

	/**
	 * COPIED: From Dynamis 5.2.0  (except for slight modifications)
	 * Utility method to return true if the field value exists and doesn't match default
	 */
	private static boolean hasValue(Object value) {
		return !(value != null && NOT_DEFINED.equals(value))
    }

	/**
	 * COPIED: From Dynamis 5.2.0  (except for slight modifications)
	 */
	private class ComparableVersion
			implements Comparable<ComparableVersion> {
		private String value

        private String canonical

        private ListItem items

        private interface Item {
			int INTEGER_ITEM = 0
            int STRING_ITEM = 1
            int LIST_ITEM = 2

            int compareTo(Item item);

			int getType();

			boolean isNull();
		}

		/**
		 * Represents a numeric item in the version item list.
		 */
		private static class IntegerItem
				implements Item {
			private static final BigInteger BIG_INTEGER_ZERO = new BigInteger("0")

            private final BigInteger value

            public static final IntegerItem ZERO = new IntegerItem()

            private IntegerItem() {
				this.value = BIG_INTEGER_ZERO
            }

            IntegerItem(String str) {
				this.value = new BigInteger(str)
            }

            int getType() {
				return INTEGER_ITEM
            }

            boolean isNull() {
				return BIG_INTEGER_ZERO.equals(value)
            }

            int compareTo(Item item) {
				if (item == null) {
					return BIG_INTEGER_ZERO.equals(value) ? 0 : 1 // 1.0 == 1, 1.1 > 1
				}

				switch (item.getType()) {
					case INTEGER_ITEM:
						return value.compareTo(((IntegerItem) item).value)

                    case STRING_ITEM:
						return 1 // 1.1 > 1-sp

					case LIST_ITEM:
						return 1 // 1.1 > 1-1

					default:
						throw new RuntimeException("invalid item: ${item.class.name}")
                }
			}

            String toString() {
				return value.toString()
            }
		}

		/**
		 * Represents a string in the version item list, usually a qualifier.
		 */
		private static class StringItem
				implements Item {
			private static final String[] QUALIFIERS = ['alpha', 'beta', 'milestone', 'rc', 'snapshot', '', 'sp' ]

            @SuppressWarnings("checkstyle:constantname")
			private static final List<String> _QUALIFIERS = Arrays.asList(QUALIFIERS)

            private static final Properties ALIASES = new Properties()
            static
			{
				ALIASES.put("ga", "")
                ALIASES.put("final", "")
                ALIASES.put("cr", "rc")
            }

			/**
			 * A comparable value for the empty-string qualifier. This one is used to determine if a given qualifier makes
			 * the version older than one without a qualifier, or more recent.
			 */
			private static final String RELEASE_VERSION_INDEX = String.valueOf(_QUALIFIERS.indexOf(""))

            private String value

            StringItem(String value, boolean followedByDigit) {
				if (followedByDigit && value.length() == 1) {
					// a1 = alpha-1, b1 = beta-1, m1 = milestone-1
					switch (value.charAt(0)) {
						case 'a':
							value = "alpha"
                            break
                        case 'b':
							value = "beta"
                            break
                        case 'm':
							value = "milestone"
                            break
                        default:
							break
                    }
				}
				this.value = ALIASES.getProperty(value, value)
            }

            int getType() {
				return STRING_ITEM
            }

            boolean isNull() {
				return (comparableQualifier(value).compareTo(RELEASE_VERSION_INDEX) == 0)
            }

			/**
			 * Returns a comparable value for a qualifier.
			 *
			 * This method takes into account the ordering of known qualifiers then unknown qualifiers with lexical
			 * ordering.
			 *
			 * just returning an Integer with the index here is faster, but requires a lot of if/then/else to check for -1
			 * or QUALIFIERS.size and then resort to lexical ordering. Most comparisons are decided by the first character,
			 * so this is still fast. If more characters are needed then it requires a lexical sort anyway.
			 *
			 * @param qualifier
			 * @return an equivalent value that can be used with lexical comparison
			 */
			static String comparableQualifier(String qualifier) {
				int i = _QUALIFIERS.indexOf(qualifier)

                return i == -1 ? (_QUALIFIERS.size() + "-" + qualifier) : String.valueOf(i)
            }

            int compareTo(Item item) {
				if (item == null) {
					// 1-rc < 1, 1-ga > 1
					return comparableQualifier(value).compareTo(RELEASE_VERSION_INDEX)
                }
				switch (item.getType()) {
					case INTEGER_ITEM:
						return -1 // 1.any < 1.1 ?

					case STRING_ITEM:
						return comparableQualifier(value).compareTo(comparableQualifier(((StringItem) item).value))

                    case LIST_ITEM:
						return -1 // 1.any < 1-1

					default:
						throw new RuntimeException("invalid item: " + item.getClass())
                }
			}

            String toString() {
				return value
            }
		}

		/**
		 * Represents a version list item. This class is used both for the global item list and for sub-lists (which start
		 * with '-(number)' in the version specification).
		 */
		private static class ListItem
				extends ArrayList<Item>
				implements Item {
			int getType() {
				return LIST_ITEM
            }

            boolean isNull() {
				return (size() == 0)
            }

			void normalize() {
				for (int i = size() - 1; i >= 0; i--) {
					Item lastItem = get(i)

                    if (lastItem.isNull()) {
						// remove null trailing items: 0, "", empty list
						remove(i)
                    } else if (!(lastItem instanceof ListItem)) {
						break
                    }
				}
			}

            int compareTo(Item item) {
				if (item == null) {
					if (size() == 0) {
						return 0 // 1-0 = 1- (normalize) = 1
					}
					Item first = get(0)
                    return first.compareTo(null)
                }
				switch (item.getType()) {
					case INTEGER_ITEM:
						return -1 // 1-1 < 1.0.x

					case STRING_ITEM:
						return 1 // 1-1 > 1-sp

					case LIST_ITEM:
						Iterator<Item> left = iterator()
                        Iterator<Item> right = ((ListItem) item).iterator()

                        while (left.hasNext() || right.hasNext()) {
							Item l = left.hasNext() ? left.next() : null
                            Item r = right.hasNext() ? right.next() : null

                            // if this is shorter, then invert the compare and mul with -1
							int result = l == null ? (r == null ? 0 : -1 * r.compareTo(l)) : l.compareTo(r)

                            if (result != 0) {
								return result
                            }
						}

						return 0

                    default:
						throw new RuntimeException("invalid item: ${item?.class.name}")
				}
			}

            String toString() {
				StringBuilder buffer = new StringBuilder()
                for (Item item : this) {
					if (buffer.length() > 0) {
						buffer.append((item instanceof ListItem) ? '-' : '.')
                    }
					buffer.append(item)
                }
				return buffer.toString()
            }
		}

        ComparableVersion(String version) {
			parseVersion(version)
        }

        final void parseVersion(String version)
		{
			this.value = version
            items = new ListItem()
            version = version.toLowerCase(Locale.ENGLISH)
            ListItem list = items
            Stack stack = new Stack<>()
            stack.push(list)
            boolean isDigit = false
            int startIndex = 0

            for (int i = 0; i < version.length(); i++)
			{
				char c = version.charAt(i)

                if (c == '.' as char)
				{
					if (i == startIndex)
					{
						list.add(IntegerItem.ZERO)
                    }
					else
					{
						list.add(parseItem(isDigit, version.substring(startIndex, i)))
                    }
					startIndex = i + 1
                }
				else if (c == '-' as char)
				{
					if (i == startIndex)
					{
						list.add(IntegerItem.ZERO)
                    }
					else
					{
						list.add(parseItem(isDigit, version.substring(startIndex, i)))
                    }
					startIndex = i + 1

                    list.add(list = new ListItem())
                    stack.push(list)
                }
				else if (Character.isDigit(c))
				{
					if (!isDigit && i > startIndex)
					{
						list.add(new StringItem(version.substring(startIndex, i), true))
                        startIndex = i

                        list.add(list = new ListItem())
                        stack.push(list)
                    }

					isDigit = true
                }
				else
				{
					if (isDigit && i > startIndex)
					{
						list.add(parseItem(true, version.substring(startIndex, i)))
                        startIndex = i

                        list.add(list = new ListItem())
                        stack.push(list)
                    }

					isDigit = false
                }
			}

			if (version.length() > startIndex)
			{
				list.add(parseItem(isDigit, version.substring(startIndex)))
            }

			while (!stack.empty)
			{
				list = (ListItem) stack.pop()
                list.normalize()
            }

			canonical = items.toString()
        }

		private static Item parseItem(boolean isDigit, String buf) {
			return isDigit ? new IntegerItem(buf) : new StringItem(buf, false)
        }

        int compareTo(ComparableVersion o) {
			return items.compareTo(o.items)
        }

        String toString() {
			return value
        }

        String getCanonical() {
			return canonical
        }

        boolean equals(Object o) {
			return (o instanceof ComparableVersion) && canonical.equals(((ComparableVersion) o).canonical)
        }

        int hashCode() {
			return canonical.hashCode()
        }
	}
}