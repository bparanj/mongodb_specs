require 'mongo'

describe 'Mongodb Map Reduce' do
  
  before do
    connection = Mongo::Connection.new
    @db = connection.db('map-reduce')  
    
    @comments = @db['comments collection']
    @comments.insert({ :text => "lmao! great article!", :author => 'kbanker', :votes => 4 })
    @comments.insert({ :text => "boring", :author => 'ghendry', :votes => 1 })
    @comments.insert({ :text => "tl:dr", :author => 'kbanker', :votes => 3 })
    @comments.insert({ :text => "best article ever", :author => 'ghendry', :votes => 2 })
    @comments.insert({ :text => "very weird", :author => 'kbanker', :votes => 2 })
    @comments.insert({ :text => "pretty good", :author => 'ghendry', :votes => 3 })
    @comments.insert({ :text => "lmao! great article!", :author => 'nstowe', :votes => 4 })
    @comments.insert({ :text => "boring", :author => 'nstowe', :votes => 1 })
    @comments.insert({ :text => "tl:dr", :author => 'nstowe', :votes => 3 })
    @comments.insert({ :text => "best article ever", :author => 'nstowe', :votes => 2 })
    @comments.insert({ :text => "very weird", :author => 'nstowe', :votes => 2 })
    @comments.insert({ :text => "pretty good", :author => 'nstowe', :votes => 3 })
    
    @map = 'function() { emit(this.author, {votes: this.votes}); }'  
    @reduce = 'function(key,values) { var sum = 0; values.forEach(function(doc) { sum += doc.votes; }); return {votes: sum}; }'
  end
  
  after do
    @db.collections.each do |collection|
      @db.drop_collection(collection.name) unless collection.name =~ /indexes$/
    end
  end
  
  context 'Basic Map Reduce' do    
    specify 'Calculate sum for a given group of specified field' do
      result = @comments.map_reduce(@map, @reduce).count
      result.should == 3
    end  

    specify '_id value will be the first one by alphabetical order' do
      result = @comments.map_reduce(@map, @reduce).find.first['_id']
      result.should == 'ghendry'
    end  

    specify 'Sum of a given field' do
      result = @comments.map_reduce(@map, @reduce).find.first['value']['votes']
      result.should == 6.0
    end  
  end
  
  context 'Map Reduce with Query' do
    
    specify 'Count the result of a query that filters using greater than syntax' do
      result = @comments.map_reduce(@map, @reduce, {:query => {:votes => {'$gt' => 1} } }).count
      result.should == 3
    end
    
    specify 'Get the value of a query that filters using greater than syntax' do
      result = @comments.map_reduce(@map, @reduce, {:query => {:votes => {'$gt' => 1} } }).find.first['value']['votes']
      result.should == 5.0
    end
    
  end
  
end
