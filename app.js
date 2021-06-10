'use strict';

const {mapUser, mapArticle, getRandomFirstName} = require('./util');

// db connection and settings
const connection = require('./config/connection');
let userCollection;
let articlesCollection;
let studentsCollection;
run();

async function run() {
  await connection.connect();
  // //await connection.get().createCollection('users')
  await connection.get().dropCollection('users');
  userCollection = connection.get().collection('users');

  // //await connection.get().createCollection('articles')
  await connection.get().dropCollection('articles');
  articlesCollection = connection.get().collection('articles');

  // //await connection.get().createCollection('students');
  await connection.get().dropCollection('students');
  studentsCollection = connection.get().collection('students');

  await example1();
  await example2();
  await example3();
  await example4();

  await hwArticles1();
  await hwArticles2();
  await hwArticles3();
  await hwArticles4();
  await hwArticles5();

  await hwStudents();
  await hwStudents1();
  await hwStudents7();

  await connection.close();
}

// #### Users

// - Create 2 users per department (a, b, c)
async function example1() {
  try {
    const departments = ['a', 'a', 'b', 'b', 'c', 'c'];
    const users = departments.map(d => ({department: d})).map(mapUser);
    try {
      const {result} = await userCollection.insertMany(users);
      console.log(`Added ${result.n} users`);
    } catch (err) {
      console.error(err);
    }
  } catch (err) {
    console.error(err);
  }
}

// - Delete 1 user from department (a)

async function example2() {
  try {
    const {result} = await userCollection.deleteOne({department: 'a'});
    console.log(`Removed ${result.n} user`);
  } catch (err) {
    console.error(err);
  }
}

// - Update firstName for users from department (b)

async function example3() {
  try {
    // const [query, update] = [{department: 'b'},{$set: {firstName: getRandomFirstName()}}]
    // const {result} = await userCollection.updateMany(query,update)
    // console.log('result',result)
    const usersB = await userCollection.find({department: 'b'}).toArray();
    const bulkWrite = usersB.map(user => ({
      updateOne: {
        filter: {_id: user._id},
        update: {$set: {firstName: getRandomFirstName()}}
      }
    }));
    const {result} = await userCollection.bulkWrite(bulkWrite);
    console.log(`Updated ${result.nModified} users`);
  } catch (err) {
    console.error(err);
  }
}

// - Find all users from department (c)
async function example4() {
  try {
    const usersC = await userCollection.find({department: 'c'}).toArray();
    console.log(`Found ${usersC.length} from department c`);
  } catch (err) {
    console.error(err);
  }
}

// ### Articles

// Create 5 articles per each type (a, b, c)
async function hwArticles1() {
  try {
    const types = ['a', 'a', 'a', 'a', 'a', 'b', 'b', 'b', 'b', 'b', 'c', 'c', 'c', 'c', 'c'];
    const articles = types.map(t => ({type: t})).map(mapArticle);
    try {
      const {result} = await articlesCollection.insertMany(articles);
      console.log(`Added ${result.n} articles`);
    } catch (err) {
      console.error(err);
    }
  } catch (err) {
    console.error(err);
  }
}

// Find articles with type a, and update tag list with next value [‘tag1-a’, ‘tag2-a’, ‘tag3’]
async function hwArticles2() {
  try {
    const [query, update] = [{type: 'a'}, {$set: {tags: ['tag1-a', 'tag2-a', 'tag3']}}];
    const {result} = await articlesCollection.updateMany(query, update);
    console.log(`Updated ${result.nModified} articles`);
  } catch (err) {
    console.error(err);
  }
}

// Add tags [‘tag2’, ‘tag3’, ‘super’] to other articles except articles from type a
async function hwArticles3() {
  try {
    const [query, update] = [
      {type: {$ne: 'a'}},
      {$push: {tags: {$each: ['tag2', 'tag3', 'super']}}}
    ];
    const {result} = await articlesCollection.updateMany(query, update);
    console.log(`Updated ${result.nModified} articles`);
  } catch (err) {
    console.error(err);
  }
}

// Find all articles that contains tags [tag2, tag1-a]
async function hwArticles4() {
  try {
    const articlesWithTags = await articlesCollection
      .find({tags: {$in: ['tag2', 'tag1-a']}})
      .toArray();
    console.log(`Found ${articlesWithTags.length} articles with tags [tag2, tag1-a]:`);
    console.log(articlesWithTags);
  } catch (err) {
    console.error(err);
  }
}

// Pull [tag2, tag1-a] from all articles
async function hwArticles5() {
  try {
    const [query, update] = [{}, {$pull: {tags: {$in: ['tag2', 'tag1-a']}}}];
    const {result} = await articlesCollection.updateMany(query, update);
    console.log(`Updated ${result.nModified} articles`);
  } catch (err) {
    console.error(err);
  }
}

// 0 - Import all data from students.json into student collection

async function hwStudents() {
  try {
    const fs = require('fs');
    const studentsData = fs.readFileSync('students.json');
    const students = JSON.parse(studentsData);
    const {result} = await studentsCollection.insertMany(students);
    console.log(`${result.n} documents have been added to collection "students"`);
  } catch (err) {
    console.error(err);
  }
}

// 1 - Find all students who have the worst score for homework, sort by descent

async function hwStudents1() {
  const NUMBER_LIMIT = 10;
  try {
    const worstHomework = await studentsCollection
      .aggregate([
        {$unwind: '$scores'},
        {$match: {'scores.type': 'homework'}},
        {$sort: {'scores.score': 1}},
        {$limit: NUMBER_LIMIT},
        {$sort: {'scores.score': -1}}
      ])
      .toArray();
    console.log(`${NUMBER_LIMIT} students, who have the worst homework scores:`);
    console.log(worstHomework);
  } catch (err) {
    console.error(err);
  }
}

// 2 - Find all students who have the best score for quiz and the worst for homework, sort by ascending
// 3 - Find all students who have best scope for quiz and exam
// 4 - Calculate the average score for homework for all students
// 5 - Delete all students that have homework score <= 60
// 6 - Mark students that have quiz score => 80

// 7 - Write a query that group students by 3 categories (calculate the average grade for three subjects)
//   - a => (between 0 and 40)
//   - b => (between 40 and 60)
//   - c => (between 60 and 100)
async function hwStudents7() {
  const NUMBER_LIMIT = 10;
  try {
    const groupScores = await studentsCollection
      .aggregate([
        {
          $project: {
            name: 1,
            scoreAvg: {$avg: ['$scores.score']}
          }
        },
        {
          $bucket: {
            groupBy: '$scoreAvg',
            boundaries: [0, 40, 60, 100],
            default: 'Out of range',
            output: {
              'Number of students': {$sum: 1},
              'List of students': {$push: '$name'}
            }
          }
        }
      ])
      .toArray();
    console.log(`Group by average:`);
    console.log(groupScores);
  } catch (err) {
    console.error(err);
  }
}
