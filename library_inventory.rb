require 'date'

# Class that returns a hash with all books from a floor's library store grouped by day and type from
# a Cassandra query result.
class LibraryInventory
  # Let's suppose we would have a Cassandra query result that looks
  # something like this (just a sample):
  # Result will return all books received for the given day:
  #  #<Cassandra::Result:0x25e47e4 @rows=[{"store_type"=>'Library', "floor"=>'1st floor',
  # "delivered"=>'2017-04-11 11:19:18', "book_type"=>'Sci-Fi'}] @last_page=true>
  def books_brought_by_type(store_type, floor, shipped_day)
    key = "#{store_type}:#{floor}"

    data = books_brought_by_day(store_type, floor, shipped_day)
    book_types = data.values[0][shipped_day]
    inventory_hash = book_types_to_hash(book_types)

    { key => { shipped_day => inventory_hash } }
    # {"Library:1st floor" => {"2016-08-11" => {"Sci-Fi" => 3, "Action" => 1, "Satire" => 2, "Science" => 2}}}
  end

  # The params would be sent to a method that uses them in a Cassandra query.
  # We are going to use mock data for now though.
  # Method will return:
  # {"Library:1st floor" => {"2016-08-11" => ["Sci-Fi", "Sci-Fi", "Sci-Fi", "Action", "Satire", "Satire", "Science", "Science"]}}
  def books_brought_by_day(store_type, floor, shipped_day)
    res = {}
    data = cassandra_query_data
    key = "#{store_type}:#{floor}"

    data.each do |v|
      book_type = v['book_type']
      res[shipped_day] = res[shipped_day].to_a << book_type
    end
    { key => res }
  end

  ## arguments:
  # - book_types : array of book types
  # Generate hash from array where each unique elem
  # from array is a new key in our hash
  # and each value represents the number of times
  # that elem is present in the array
  def book_types_to_hash(book_types)
    result = {}
    book_types.each do |book_type|
      key = book_type.to_s
      result[key] = result[key].to_i + 1
    end
    result
  end

  def cassandra_query_data
    [{ 'store_type' => 'Library', 'floor' => '1st floor', 'delivered' => '2017-04-11 11:19:18', 'book_type' => 'Sci-Fi' }, { 'store_type' => 'Library', 'floor' => '1st floor', 'delivered' => '2017-04-11 11:19:21', 'book_type' => 'Sci-Fi' }, { 'store_type' => 'Library', 'floor' => '1st floor', 'delivered' => '2017-04-11 11:19:59', 'book_type' => 'Sci-Fi' }, { 'store_type' => 'Library', 'floor' => '1st floor', 'delivered' => '2017-04-11 11:20:33', 'book_type' => 'Action' }, { 'store_type' => 'Library', 'floor' => '1st floor', 'delivered' => '2017-04-11 13:20:36', 'book_type' => 'Satire' }, { 'store_type' => 'Library', 'floor' => '1st floor', 'delivered' => '2017-04-11 13:20:39', 'book_type' => 'Satire' }, { 'store_type' => 'Library', 'floor' => '1st floor', 'delivered' => '2017-04-11 13:21:15', 'book_type' => 'Science' }, { 'store_type' => 'Library', 'floor' => '1st floor', 'delivered' => '2017-04-11 13:21:21', 'book_type' => 'Science' }]
  end
end

inventory = LibraryInventory.new
puts inventory.books_brought_by_type('Library', '1st floor', '2016-08-11')
