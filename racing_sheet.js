#!/usr/bin/env node
'use strict';

var Moment = require('moment-timezone');
const SHEET_KEY = '1LI4OBEGMPKxBwtGY_xJ1yT_G-rrCJxl8FaC0q0DaBl0';

const TS_PATH = './node_modules/jstrueskill/lib/racingjellyfish/jstrueskill';
var TrueSkillCalculator = require(`${TS_PATH}/TrueSkillCalculator`);
var Player = require(`${TS_PATH}/Player`);
var Team = require(`${TS_PATH}/Team`);
let GameInfo = new(require(`${TS_PATH}/GameInfo`))
      (25.0,           // Mean for new players (Default: 25)
       25.0 / 3.0,     //Standard deviatation  for new players (Defualt: Mean/3)
       25.0 / 2.0,     //Beta - Number of skill points for 80% chance to win (Default: Mean/6)
       25.0 / 150.0,   //Tau - amount added to a user's SD before each match (Default: Mean/300)
       0.1);           //Draw probability (changing this caused the algorithm to fail more freq.)

var _ = require('lodash');

const DNF =       Number.MAX_SAFE_INTEGER / 2;
const UNKNOWN =   Number.MAX_SAFE_INTEGER;

function getSheetRows(sheet, tab) {
  return new Promise((resolve,reject) => {
    sheet.getRows(tab, (err, rows) => {
      if (err !== null) {reject(err);}
      resolve(rows);
    });
  });
}

//Participants
function getParticipants(rows) {
  return _(rows)
  .map((row) => {
    return {
      driverId: row.participantid,
      fullname: `${row.last}, ${row.first}`,
      driverSkill: GameInfo.getDefaultRating()
    };
  })
  .keyBy('driverId')
  .value();
}

//Races
function getRaces(rows) {
  return _(rows)
  .map((row) => {
    return {
      raceId: row.raceid,
      name: `${row.year} ${row.racename}`,
      theme: row.theme,
      abbreviation: row.raceabbr,
      date: Moment.tz(row.date, 'MM/DD/YYYY', 'America/New_York').toJSON(),
      numLegs: Number.parseInt(row.numlegs),
      legs: [],
      toScore: row.toscore === 'TRUE' ? true : false,
      start: row.start,
      finish: row.finish,
      distance: row.distance
    };
  })
  .keyBy('raceId')
  .value();
}

function getLegResults(rows, races) {
  let results = rows.map((row) => {
    const RACE_DATE = Moment(races[row.raceid].date);
    return {
      raceId: row.raceid,
      leg: Number.parseInt(row.leg),
      driverId: Number.parseInt(row.driverid),
      navigatorId: 'n/a',
      start: RACE_DATE.clone().add(Moment.duration(row.starttime)).toJSON(),
      end: RACE_DATE.clone().add(Moment.duration(row.finishtime)).toJSON(),
      duration: Moment.duration(row.legduration).toJSON(),
      rank: null, //placeholder
      dnf: row.dnf === 'TRUE' ? true : false,
      unknownTime: row.unknown === 'TRUE' ? true : false
    };
  });
  return results;
}

function getResultCompare(r) {
  if (r.dnf === true) { return DNF; }
  if (r.unknownTime === true) { return UNKNOWN; }
  return Moment.duration(r.duration).asMilliseconds();
}

function rankResults(results) {
  return _(results)
  .groupBy(getResultCompare)
  .toPairs()
  .sortBy(result => Number(result[0]))
  //put these groups back into the result time array, adding their rank
  .reduce((prevResults, curResultGroup) => {
    curResultGroup[1].forEach(result => {
      result.rank = prevResults.length + 1;
    });
    return _.concat(prevResults, curResultGroup[1]);
  }, []);
}

function scoreLeg(legResults, participants) {

  //HACK: this doesn't actually impact the legResults the results are put
  //in the particpant data.  That feels like poor practice to me, but, eh....

  console.log(`Scoring ${legResults[0].raceId}, Leg ${legResults[0].leg}`);

  if (legResults.length < 2) {return;}

  let skillData = _(legResults)
  .reject('unknownTime')
  .reduce((results, legResult) => {
    let driverId = legResult.driverId;
    results.driverIds.push(driverId);
    results.teams.push(new Team(driverId.toString(),
                       new Player(driverId),
                       participants[driverId].driverSkill));
    results.ranks.push(legResult.rank);

    if (driverId === 68) {
      console.log(`Rank: ${legResult.rank}`);
    }
    return results;
  }, {driverIds: [], teams: [], ranks: []});

  let newSkills;
  let ratingsCalculated = false;
  while (!ratingsCalculated) {
    try {
      newSkills = TrueSkillCalculator.calculateNewRatings(GameInfo,
                  skillData.teams, skillData.ranks);
      ratingsCalculated = true;
    } catch (err) {
      /*jshint loopfunc: true */
      let lastRanking = _.last(skillData.ranks);
      let indexToChange = _.findLastIndex(skillData.ranks, rank => rank !== lastRanking);
      skillData.ranks[indexToChange] = lastRanking;
      let driverName = participants[skillData.driverIds[indexToChange]].fullname;
      console.error(`SCORING ERROR: Changing the rating of: ${driverName}`);
    }
  }
  skillData.teams.forEach(team => {
    let player = team.getPlayers()[0];
    participants[player.getId()].driverSkill = newSkills[player];
    if (Number(player.getId()) === 68) {
      console.log(JSON.stringify(newSkills[player]));
    }

  });

  return legResults;
}

function getRaceResults(legResults, participants, races) {
  console.log(legResults.length);
  console.log(_(legResults).groupBy(r => {
    return `${r.raceId} ${r.leg} ${getResultCompare(r)}`;
  }).keys().value().length);

  return _.chain(legResults)

  // HACK: puts the  legs date order...  so that we score the race results in Date
  // order (needed for Trueskill).  I couldn't figure out how to order the grouping
  // below....
  .sortBy(leg => races[leg.raceId].date)

  .groupBy('raceId')
  .mapValues(race => {
    // console.log(race);
    return _.chain(race)

    ///
    //STEP 1: RANK THE LEGS
    //
    .groupBy('leg') //groups the results by eaech leg
    .mapValues(rankResults) //ranks each leg
    .flatMap(legs => scoreLeg(legs, participants))

    ///
    //STEP 2: BUILD A RESULT TABLE FOR EACH RACE
    //
    .groupBy('driverId')
    .mapValues((legs, driverId) => {
      let driverResult = {
        driverId: driverId,
        driverName: participants[driverId].fullname,
        navigatorName: 'n/a',
        rank: null,

        duration: _.reduce(legs, (duration, leg) => {
          return duration.add(Moment.duration(leg.duration));
        }, Moment.duration(0)).toJSON(),

        legs: _.map(legs, leg =>
          _.pick(leg, ['start','end', 'duration', 'rank', 'dnf', 'unknownTime'])),

        // These two parameters are TRUE at the race level if they are true for any leg
        dnf: !(_.every(legs, ['dnf', false])),
        unknownTime: !(_.every(legs, ['unknownTime', false])),
      };
      return driverResult;
    })
    .value();
  })
  //Rank the race results
  .mapValues(rankResults)
  ///
  //STEP 3: ADD CONSOLIDATED INFORMATION ABOUT EACH RACE
  //
  .mapValues((results, raceId) => {
    return _(races[raceId])
    .pick(['raceId', 'name', 'theme', 'date', 'start', 'end', 'distance', 'numLegs', 'toScore'])
    .merge({
      results: results,
      time: results[0].duration,
      winner: results[0].driverName
    })
    .value();
  })
  .value();
}

function getRaceList(raceResults) {
  return _(raceResults)
  .flatMap(result => _.omit(result, ['results']))
  .orderBy('date')
  .value();
}

function getDriverStats(raceResults, participants) {

  return _(raceResults)
  .flatMap('results')
  .groupBy('driverId')
  .mapValues((driverResults, driverId) => {
    return {
      id: driverId,
      name: participants[driverId].fullname,
      starts: driverResults.length,
      finishes: _.filter(driverResults, result => result.dnf === false).length,
      wins: _.filter(driverResults, result => result.rank === 1).length,
      podiums: _.filter(driverResults, result => result.rank <= 3).length,
      driverSkill: _.round(participants[driverId].driverSkill.conservativeRating, 2)
    };
  })
  .orderBy('driverSkill', 'desc')
  .value();
}

//output the ranked list of drivers
function printDriverStats(driverStats) {
  _(driverStats)
  .forEach((driver, index) => {
    console.log(`${index}\t${driver.driverSkill}\t${driver.starts}\t${driver.name}`);
  });
}

function main() {
  console.log('Getting data from Google Sheets');
  // spreadsheet key is the long id in the sheets URL
  var GoogleSpreadsheet = require('google-spreadsheet');
  var Sheet = new GoogleSpreadsheet(SHEET_KEY);

  Promise.all([getSheetRows(Sheet, 1).then(getParticipants),
               getSheetRows(Sheet, 3).then(getRaces),
               getSheetRows(Sheet, 2)
             ])
  .then((promiseResults) => {
    console.log('Data from sheets pulled');
    let participants = promiseResults[0];
    let races = promiseResults[1];
    let legResults = getLegResults(promiseResults[2], races);

    let raceResults = getRaceResults(legResults, participants, races);
    let raceList = getRaceList(raceResults);

    let driverStats = getDriverStats(raceResults, participants);

    //scoreResults(raceResults, participants);

    console.log('Updating Firebase');
    var Firebase = require('firebase');
    let fbRef = new Firebase('https://dttdata.firebaseio.com/');
    fbRef.set({
      races: raceList,
      raceResults: raceResults,
      driverStats: driverStats
    });

    printDriverStats(driverStats);

    console.log('all done');
  })
  .catch(err => {
    console.error(`ERROR: ${err}`);
    console.error(err.stack);
    throw err;
  });
}

main();
