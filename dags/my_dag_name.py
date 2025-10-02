from airflow.sdk import Variable

# Normal call style
foo = Variable.get("fool")

#Auto-deserializes a JSON value
bar = Variable.get("bar", deserialize_json=True)

# Returns a default value if the variable is not set
baz = Variable.get("baz", default_var="default_value")