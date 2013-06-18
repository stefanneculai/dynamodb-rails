class Model
	include Dynamo::Model
	
	table :test_uniq
	field :mf
	field :id
	
	key :hash, :id, :S
end
