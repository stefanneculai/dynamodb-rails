class Model
	include Dynamo::Model
	
	table :test_range
	field :mf
	field :id
  field :created_at
	
	key :hash, :id, :S
  key :range, :created_at, :N

  index :mf, :S
end
