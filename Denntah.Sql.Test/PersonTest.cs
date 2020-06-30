using System;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using Denntah.Sql.Test.Models;
using Xunit;

namespace Denntah.Sql.Test
{
    [Collection("DBTest")]
    public class PersonTest : IDisposable
    {
        private DbConnection _db;
        private Person _data;

        public PersonTest()
        {
            _db = DatabaseFactory.Connect();

            _data = new Person
            {
                FirstName = "Foo",
                LastName = "Bar",
                Gender = Gender.Male,
                DateCreated = DateTime.Now
            };
        }

        [Fact]
        public void Insert()
        {
            int id = _db.Insert<int>("persons", _data, "id");

            Assert.True(id > 0);
        }

        [Fact]
        public async Task InsertAsync()
        {
            int id = await _db.InsertAsync<int>("persons", _data, "id").ConfigureAwait(false);

            Assert.True(id > 0);
        }

        [Fact]
        public void Upsert()
        {
            _data.FirstName = "Gandalf";
            _data.Age = 1000;
            _data.Id = _db.Insert<int>("persons", _data, "id");

            // Will create new as pk is generated
            _data.FirstName = "Saruman";
            _data.Age = 2000;
            _db.Upsert("persons", _data, "id");

            Person person = _db.Query<Person>("SELECT * FROM persons WHERE id=@Id", _data).FirstOrDefault();

            Assert.NotNull(person);
            Assert.Equal(_data.Id, person.Id);
            Assert.NotEqual(person.FirstName, _data.FirstName);
            Assert.NotEqual(person.Age, _data.Age);
        }

        [Fact]
        public async Task UpsertAsync()
        {
            _data.FirstName = "Gandalf";
            _data.Age = 1000;
            _data.Id = await _db.InsertAsync<int>("persons", _data, "id").ConfigureAwait(false);

            // Will create new as pk is generated
            _data.FirstName = "Saruman";
            _data.Age = 2000;
            _db.Upsert("persons", _data, "id");

            Person person = (await _db.QueryAsync<Person>("SELECT * FROM persons WHERE id=@Id", _data).FirstOrDefaultAsync().ConfigureAwait(false));

            Assert.NotNull(person);
            Assert.Equal(_data.Id, person.Id);
            Assert.NotEqual(person.FirstName, _data.FirstName);
            Assert.NotEqual(person.Age, _data.Age);
        }

        [Fact]
        public void Update()
        {
            _data.Id = _db.Insert<int>("persons", _data, "id");
            _data.FirstName = "Baz";
            _data.Age = 20;

            int affected = _db.Update("persons", _data, "id=@Id");

            Person updated = _db.Query<Person>("SELECT * FROM persons WHERE id=@Id", _data).FirstOrDefault();

            Assert.NotNull(updated);
            Assert.Equal("Baz", updated.FirstName);
            Assert.Equal(20, updated.Age);
        }

        [Fact]
        public async Task UpdateAsync()
        {
            _data.Id = await _db.InsertAsync<int>("persons", _data, "id").ConfigureAwait(false);
            _data.FirstName = "Baz";
            _data.Age = 20;

            int affected = await _db.UpdateAsync("persons", _data, "id=@Id").ConfigureAwait(false);

            Person updated = (await _db.QueryAsync<Person>("SELECT * FROM persons WHERE id=@Id", _data).FirstOrDefaultAsync().ConfigureAwait(false));

            Assert.NotNull(updated);
            Assert.Equal("Baz", updated.FirstName);
            Assert.Equal(20, updated.Age);
        }

        [Fact]
        public void Delete()
        {
            _data.Id = _db.Insert<int>("persons", _data, "id");

            int affected = _db.Delete("persons", "id=@id", _data);

            Assert.Equal(1, affected);
        }

        [Fact]
        public async Task DeleteAsync()
        {
            _data.Id = await _db.InsertAsync<int>("persons", _data, "id").ConfigureAwait(false);

            int affected = await _db.DeleteAsync("persons", "id=@id", _data).ConfigureAwait(false);

            Assert.Equal(1, affected);
        }

        [Fact]
        public void Query()
        {
            _data.Id = _db.Insert<int>("persons", _data, "id");

            Person person = _db.Query<Person>("SELECT * FROM persons WHERE id=@Id", _data).FirstOrDefault();

            Assert.NotNull(person);
            Assert.Equal(_data.Id, person.Id);
            Assert.Equal(_data.FirstName, person.FirstName);
            Assert.Equal(_data.Age, person.Age);
            Assert.Equal(_data.Gender, person.Gender);
            Assert.Equal(_data.DateCreated.ToString(), person.DateCreated.ToString());
        }

        [Fact]
        public async Task QueryAsync()
        {
            _data.Id = await _db.InsertAsync<int>("persons", _data, "id").ConfigureAwait(false);

            Person person = (await _db.QueryAsync<Person>("SELECT * FROM persons WHERE id=@Id", _data).FirstOrDefaultAsync().ConfigureAwait(false));

            Assert.NotNull(person);
            Assert.Equal(_data.Id, person.Id);
            Assert.Equal(_data.FirstName, person.FirstName);
            Assert.Equal(_data.Age, person.Age);
            Assert.Equal(_data.Gender, person.Gender);
            Assert.Equal(_data.DateCreated.ToString(), person.DateCreated.ToString());
        }

        [Fact]
        public void QueryAssoc()
        {
            _data.Id = _db.Insert<int>("persons", _data, "id");

            var person = _db.QueryAssoc("SELECT * FROM persons WHERE id=@Id", _data).FirstOrDefault();

            Assert.NotNull(person);
            Assert.Equal(_data.Id, int.Parse(person["id"].ToString()));
            Assert.Equal(_data.FirstName, person["first_name"]);
            Assert.Equal(_data.Age, person["age"]);
            Assert.Equal(_data.Gender, person["gender"]);
            Assert.Equal(_data.DateCreated.ToString(), person["date_created"].ToString());
        }

        [Fact]
        public async Task QueryAssocAsync()
        {
            _data.Id = await _db.InsertAsync<int>("persons", _data, "id").ConfigureAwait(false);

            var person = (await _db.QueryAssocAsync("SELECT * FROM persons WHERE id=@Id", _data).FirstOrDefaultAsync().ConfigureAwait(false));

            Assert.NotNull(person);
            Assert.Equal(_data.Id, int.Parse(person["id"].ToString()));
            Assert.Equal(_data.FirstName, person["first_name"]);
            Assert.Equal(_data.Age, person["age"]);
            Assert.Equal(_data.Gender, person["gender"]);
            Assert.Equal(_data.DateCreated.ToString(), person["date_created"].ToString());
        }

        [Fact]
        public void QueryArray()
        {
            _data.Id = _db.Insert<int>("persons", _data, "id");

            var person = _db.QueryArray("SELECT id,first_name,age,gender,date_created FROM persons WHERE id=@Id", _data).FirstOrDefault();

            Assert.NotNull(person);
            Assert.Equal(_data.Id, int.Parse(person[0].ToString()));
            Assert.Equal(_data.FirstName, person[1]);
            Assert.Equal(_data.Age, person[2]);
            Assert.Equal(_data.Gender, person[3]);
            Assert.Equal(_data.DateCreated.ToString(), person[4].ToString());
        }

        [Fact]
        public async Task QueryArrayAsync()
        {
            _data.Id = await _db.InsertAsync<int>("persons", _data, "id").ConfigureAwait(false);

            var person = (await _db.QueryArrayAsync("SELECT id,first_name,age,gender,date_created FROM persons WHERE id=@Id", _data).FirstOrDefaultAsync().ConfigureAwait(false));

            Assert.NotNull(person);
            Assert.Equal(_data.Id, int.Parse(person[0].ToString()));
            Assert.Equal(_data.FirstName, person[1]);
            Assert.Equal(_data.Age, person[2]);
            Assert.Equal(_data.Gender, person[3]);
            Assert.Equal(_data.DateCreated.ToString(), person[4].ToString());
        }

        public void Dispose()
        {
            _db.Dispose();
        }
    }
}