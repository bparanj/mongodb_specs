require 'mongo'

describe 'Mongodb Queries' do
  before do
    connection = Mongo::Connection.new
    @db = connection.db('queries')  
  end
  
  after do
    @db.collections.each do |collection|
      @db.drop_collection(collection.name) unless collection.name =~ /indexes$/
    end
  end
  
  context 'Data set one' do
    before do
      @people = @db["people"]
      @people.insert({:name => "Ada", :active => false})
      @people.insert({:name => "Bob", :active => false})
      @people.insert({:name => "Cathy", :active => true})
      @people.insert({:name => "Dan", :active => true})
    end
    
    specify 'find() function on a collection retrieves all of the documents. The count() function returns the number of documents in the specified collection' do
      all_people = @people.find.count

      all_people.should == 4
    end

    specify 'Find by name : Non existing case' do
      result = @people.find(:name => "Adam").count

      result.should == 0
    end

    specify 'Find all documents matching the value for a given field' do
      result = @people.find(:name => "Ada").count

      result.should == 1
    end
    
    specify 'All documents are automatically indexed on the _id key' do
      number_of_indexes = @people.index_information.count
      
      number_of_indexes.should == 1
    end
    
    specify 'Getting a list of indexes on a collection.' do
      indexes = @people.index_information

      indexes.should_not be_empty
    end

    specify 'Create Index on a field.' do
      @people.create_index('name')
      number_of_indexes = @people.index_information.count
      
      number_of_indexes.should == 2
    end
    
    specify 'Find by index field' do
      @people.create_index('active')
      result = @people.find(:active => true).count
      
      result.should == 2
    end

    specify 'Find by given field value, skip is ignored when it is used with count' do
      result = @people.find(:active => true).skip(1).count
      
      result.should == 2
    end

    specify 'Find by given field value, skip is forced when it is used with count(true)' do
      result = @people.find(:active => true).skip(1).count(true)
      
      result.should == 1
    end

    specify 'Use explain method on the cursor to show how MongoDB will run the query' do
      id = @people.insert({:name => "Bugs"})
      result = @people.find("_id" => id).explain
      # Query by _id will use faster indexed BtreeCursor
      result['cursor'].should == "BtreeCursor _id_"
    end

    specify 'Use explain : Query by a given field will use slower BasicCursor' do
      id = @people.insert({:name => "Bugs"})
      result = @people.find("name" => "Bugs").explain
      # Query by a given field will use slower BasicCursor
      result['cursor'].should == "BasicCursor"
    end

    specify 'Find by index field : Using explain[\'nscanned\'] to count' do
      total_people = @people.count
      total_people.should == 4
      
      @people.create_index('active')
      result = @people.find(:active => true).explain['nscanned']
      
      result.should == 2
    end
    
    specify 'Retrieve a document with slice' do
      pending 'Refer for the right syntax to use slice'
      result = @people.find({'name' => {'$slice' => 1}})
      
      result.count.should == 0
    end
  end
  
  context 'Data set two' do
    before do
      @books = @db["books"]
      @books.insert({:name => "Pickaxe", :author => "Dave", :hard_cover => false})
      @books.insert({:name => "Patterns", :author => "Russ", :hard_cover => true})
      @books.insert({:name => "Refactoring", :author => "Jay", :hard_cover => true})
      @books.insert({:name => "Refactoring", :author => "William", :hard_cover => false})      
    end
    
    specify 'Find : Selecting a subset of fields for a query. THIS IS A BUG' do
      result = @books.find(:name => "Pickaxe", :fields => ['author'] ).to_a
      # p result
      result.should == []
    end

    specify 'Find by field1 and field2' do
      result = @books.find({:name => "Refactoring", :hard_cover => true }).count

      result.should == 1
    end
    #Which index does each query use?
    specify 'Sorted query' do
      @books.create_index([['name', Mongo::DESCENDING], ['author', Mongo::DESCENDING]])  
      
      name = @books.find({:name => "Refactoring"}).first['author']
      name.should == 'William'
      
      first_author = @books.find.first['author']
      first_author.should == 'Dave'
    end    
    
    specify 'Distinct query' do
      result = @books.distinct(:name).count
      
      result.should == 3
    end
  end
  
  context 'Data set three' do
    specify 'Multikey index' do
      @articles = @db['articles']
      @articles.save( { :name => "Spring", :author => "Steve", :tags => ['weather', 'hot', 'record', 'april'] } )
      @articles.save( { :name => "Winter", :author => "Steve", :tags => ['weather', 'cold', 'snow'] } )

      @articles.create_index('tags')

      result = @articles.find('tags' => 'cold').count

      result.should == 1
    end
  end  
  
end