require 'mongo'
require 'date'

describe 'Basics of Mongodb Data types' do
  before do
    connection = Mongo::Connection.new
    @db = connection.db('data_types')
    @collection = @db['my_collection']
  end
  
  after do
    @db.collection_names.each do |name|
      @db.drop_collection(name) unless name =~ /indexes$/
    end
  end
  
  specify 'When you store int data type in Mongodb, the find_one returns Fixnum' do
    @collection.insert({:value => 1234 })
    result = @collection.find_one['value']
    
    result.should be_instance_of(Fixnum)
  end
  
  specify 'When you store float data type in Mongodb, the find_one returns Float' do
    @collection.insert({:value => 123.4 })
    result = @collection.find_one['value']
    
    result.should be_instance_of(Float)
  end

  specify 'When you store string data type in Mongodb, the find_one returns String' do
    @collection.insert({:value => 'hi' })
    result = @collection.find_one['value']
    
    result.should be_instance_of(String)
  end

  specify 'When you store time data type in Mongodb, the find_one returns Time' do
    @collection.insert({:value => Time.new })
    result = @collection.find_one['value']
    
    result.should be_instance_of(Time)
  end
  
  specify 'Date cannot be a type that can be stored' do
    expect do
      @collection.insert({:value => Date.new })  
    end.to raise_error
  end
  
  specify 'DateTime cannot be a type that can be stored' do
    expect do
      @collection.insert({:value => DateTime.new })  
    end.to raise_error
  end

  specify 'When you store false boolean data type in Mongodb, the find_one returns FalseClass' do
    @collection.insert({:value => false })
    result = @collection.find_one['value']
    
    result.should be_instance_of(FalseClass)
  end

  specify 'When you store true boolean data type in Mongodb, the find_one returns TrueClass' do
    @collection.insert({:value => true })
    result = @collection.find_one['value']
    
    result.should be_instance_of(TrueClass)
  end

  specify 'When you store nil in Mongodb, the find_one returns NilClass' do
    @collection.insert({:value => nil })
    result = @collection.find_one['value']
    
    result.should be_instance_of(NilClass)
  end

  specify 'When you access a key that does not exist in Mongodb, the find_one returns NilClass' do
    @collection.insert({:value => 'xyz' })
    result = @collection.find_one['non-existing-key']
    
    result.should be_instance_of(NilClass)
  end
  
  specify 'When you store Array in Mongodb, the find_one returns Array' do
    @collection.insert({:value => [1,2] })
    result = @collection.find_one['value']
    
    result.should be_instance_of(Array)
  end
  
  specify 'When you store any document in Mongodb, the find_one returns BSON::ObjectId for the _id attribute' do
    @collection.insert({:value => 1234 })
    result = @collection.find_one['_id']
    
    result.should be_instance_of(BSON::ObjectId)
  end
  
  specify 'When you store regex in Mongodb, the find_one returns Regexp' do
    @collection.insert({:value => /^123$/i })
    result = @collection.find_one['value']
    
    result.should be_instance_of(Regexp)
  end
  
  specify 'MongoDB also has data types binary, cstr, code, object' do
    true.should be_true
  end
  
end
    