require 'mongo'

describe 'Mongodb Learning Specs using Mongo Ruby driver' do
  context 'Basics of Mongodb' do
    
    specify 'Create a connection to MongoDB' do
      connection = Mongo::Connection.new

      connection.should be_instance_of(Mongo::Connection)      
    end
    
    specify 'Create a new database named matrix' do
      connection = Mongo::Connection.new
      db = connection.db('matrix')

      db.should be_instance_of(Mongo::DB)
    end

    specify 'Create a new database named matrix, another way.' do
      db = Mongo::Connection.new.db('matrix')

      db.should be_instance_of(Mongo::DB)
    end

    specify 'Create an empty collection called empty in matrix database' do
      connection = Mongo::Connection.new
      db = connection.db('matrix')

      empty_collection = db.create_collection('empty')

      empty_collection.size.should == 0
      empty_collection.should be_instance_of(Mongo::Collection)
    end  
    
    specify 'Listing all databases' do
      connection = Mongo::Connection.new
      result = connection.database_names
      
      result.should include('local')
      result.should include('admin')
    end
    
    specify 'List all the databases and its size' do
      connection = Mongo::Connection.new
      result = connection.database_info
      
      result.should include("admin" => 1)
      result.should include("local" => 1)
    end
    
    specify 'You can get a collection to use using the collection method' do
      db = Mongo::Connection.new.db('matrix')
      collection = db.collection('my_collection')
      
      collection.should be_instance_of(Mongo::Collection)
    end
    
    specify 'You can get a collection to use using the shortcut [] method' do
      db = Mongo::Connection.new.db('matrix')
      collection = db['my_collection']
      
      collection.should be_instance_of(Mongo::Collection)
    end
    
  end
  
  context 'About CRUD' do
    before do
      @connection = Mongo::Connection.new
      @db = @connection.db('matrix')
      @collection = @db['my_collection']
    end
    
    after do
      @db.collections.each do |collection|
        @db.drop_collection(collection.name) unless collection.name =~ /indexes$/
      end
    end
    
    specify 'Insert a document in matrix database' do
      document = {:test => 'hello mongo'}
      @collection.insert(document)

      @collection.size.should == 1
      document[:test].should == @collection.find_one['test']
    end


    specify 'Each database has 0 or more collections. You can retrieve a list of them from the db' do
      document = {:test => 'hello mongo'}
      @collection.insert(document)

      result = @db.collection_names
      result.should == ['system.indexes', 'my_collection']
    end
    
    specify 'Reading the first document in a collection using find_one. Use find_one if you expect only one result' do
      document = {:test => 'hello mongo'}
      @collection.insert(document)

      @collection.find_one['test'].should == 'hello mongo'
    end
    
    specify 'Replace a document' do      
      document = {:category => "sandwich"}
      @collection.insert(document)
      
      existing_document = @collection.find_one
      existing_document['type'] = 'peanut butter'
      @collection.save(existing_document)
      
      @collection.size.should == 1
      @collection.find_one['category'].should == 'sandwich'
      @collection.find_one['type'].should == 'peanut butter'
    end
    # Using variable document to make it clear that we are inserting document
    specify 'The update() function takes four arguments: criteria, objNew, upsert and multi' do
      document = {:category => "sandwich", :type => "peanut butter" }
      @collection.insert(document)
      # The criteria specifies the query that selects the record to update : {'category' => 'sandwich'}
      # The objNew specifies the updated information : {'$set' => {:type => "tuna fish"}
      @collection.update({'category' => 'sandwich'}, {'$set' => {:type => "tuna fish"}})
      
      @collection.find_one['type'].should == 'tuna fish'
      @collection.find_one['category'].should == 'sandwich'
    end
    # Instead of using document1 and document2, using sandwich and soup for document names
    specify 'Delete a document with remove' do
      sandwich = {:category => "sandwich", :type => "peanut butter" }
      @collection.insert(sandwich)
      soup = {:category => "soup", :type => "minestrone" }
      @collection.insert(soup)
      @collection.size.should == 2
      
      @collection.remove({'category' => 'sandwich'})
      
      @collection.size.should == 1
    end
    
    specify 'Using a hash key : Mongo driver only allows symbols as hash keys in some commands' do
      sandwich = {:category => "sandwich", :type => "peanut butter" }
      @collection.insert(sandwich)
      
      @collection.find_one['type'].should == 'peanut butter'
    end
  end
end