#!/usr/bin/env node
'use strict';

const Moment = require('moment-timezone');
const SHEET_KEY = '1LI4OBEGMPKxBwtGY_xJ1yT_G-rrCJxl8FaC0q0DaBl0';

const TS_PATH = './node_modules/jstrueskill/lib/racingjellyfish/jstrueskill';
const TrueSkillCalculator = require(`${TS_PATH}/TrueSkillCalculator`);
const Player = require(`${TS_PATH}/Player`);
const Team = require(`${TS_PATH}/Team`);
const GameInfo = new (require(`${TS_PATH}/GameInfo`))(
       25.0,           // Mean for new players (Default: 25)
       25.0 / 3.0,     // Standard deviatation  for new players (Defualt: Mean/3)
       25.0 / 2.0,     // Beta - Number of skill points for 80% chance to win (Default: Mean/6)
       25.0 / 150.0,   // Tau - amount added to a user's SD before each match (Default: Mean/300)
       0.1);           // Draw probability (changing this caused the algorithm to fail more freq.)

const _ = require('lodash');

const DNF = Number.MAX_SAFE_INTEGER / 2;
const UNKNOWN = Number.MAX_SAFE_INTEGER;

function getSheetRows(sheet, tab) {
  return new Promise((resolve, reject) => {
    sheet.getRows(tab, (err, rows) => {
      if (err !== null) {
        reject(err);
      }
      resolve(rows);
    });
  });
}

// Participants
function getParticipants(rows) {
  return _(rows)
  .map(row => {
    return {
      driverId: row.participantid,
      fullname: `${row.last}, ${row.first}`,
      driverSkill: null,
      driverSkills: null
    };
  })
  .keyBy('driverId')
  .value();
}

// Races
function getRaces(rows) {
  return _(rows)
  .map(row => {
    return {
      raceId: row.raceid,
      name: `${row.year} ${row.racename}`,
      theme: row.theme,
      abbreviation: row.raceabbr,
      date: Moment.tz(row.date, 'MM/DD/YYYY', 'America/New_York').toJSON(),
      numLegs: Number.parseInt(row.numlegs, 10),
      legs: [],
      toScore: row.toscore === 'TRUE',
      start: row.start,
      finish: row.finish,
      distance: row.distance
    };
  })
  .keyBy('raceId')
  .value();
}

function getLegResults(rows, races) {
  const results = rows.map(row => {
    const RACE_DATE = new Moment(races[row.raceid].date);
    return {
      raceId: row.raceid,
      leg: Number.parseInt(row.leg, 10),
      driverId: Number.parseInt(row.driverid, 10),
      navigatorId: 'n/a',
      start: RACE_DATE.clone().add(Moment.duration(row.starttime)).toJSON(),
      end: RACE_DATE.clone().add(Moment.duration(row.finishtime)).toJSON(),
      duration: Moment.duration(row.legduration).toJSON(),
      rank: null, // placeholder
      dnf: row.dnf === 'TRUE',
      unknownTime: row.unknown === 'TRUE'
    };
  });
  return results;
}

function getResultCompare(r) {
  if (r.dnf === true) {
    return DNF;
  }
  if (r.unknownTime === true) {
    return UNKNOWN;
  }
  return Moment.duration(r.duration).asMilliseconds();
}

function rankResults(results) {
  return _(results)
  .groupBy(getResultCompare)
  .toPairs()
  .sortBy(result => Number(result[0]))
  // put these groups back into the result time array, adding their rank
  .reduce((prevResults, curResultGroup) => {
    curResultGroup[1].forEach(result => {
      result.rank = prevResults.length + 1;
    });
    return _.concat(prevResults, curResultGroup[1]);
  }, []);
}

function getRaceResults(legResults, participants, races) {
  return _.chain(legResults)

  // HACK: puts the  legs date order...  so that we score the race results in Date
  // order (needed for Trueskill).  I couldn't figure out how to order the grouping
  // below....
  .sortBy(leg => races[leg.raceId].date)

  .groupBy('raceId')
  .mapValues(race => {
    // console.log(race);
    return _.chain(race)

    //
    // STEP 1: RANK THE LEGS
    //
    .groupBy('leg') // groups the results by eaech leg
    .flatMap(rankResults) // ranks each leg
    // .flatMap(legs => scoreLeg(legs, participants))

    //
    // STEP 2: BUILD A RESULT TABLE FOR EACH RACE
    //
    .groupBy('driverId')
    .mapValues((legs, driverId) => {
      const driverResult = {
        driverId: driverId,
        driverName: participants[driverId].fullname,
        navigatorName: 'n/a',
        rank: null,

        duration: _.reduce(legs, (duration, leg) => {
          return duration.add(Moment.duration(leg.duration));
        }, Moment.duration(0)).toJSON(),

        legs: _.map(legs, leg =>
          _.pick(leg, ['start', 'end', 'duration', 'rank',
                       'dnf', 'unknownTime'])),

        // These two parameters are TRUE at the race level if they are true for any leg
        dnf: !(_.every(legs, ['dnf', false])),
        unknownTime: !(_.every(legs, ['unknownTime', false]))
      };
      return driverResult;
    })
    .value();
  })
  // Rank the race results
  .mapValues(rankResults)
  //
  // STEP 3: ADD CONSOLIDATED INFORMATION ABOUT EACH RACE
  //
  .mapValues((results, raceId) => {
    return _(races[raceId])
    .pick(['raceId', 'name', 'theme', 'date', 'start', 'end',
           'distance', 'numLegs', 'toScore'])
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
      driverSkill: participants[driverId].driverSkill,
      driverSkills: participants[driverId].driverSkills
    };
  })
  .orderBy('driverSkill.conservativeRating', 'desc')
  .value();
}

function scoreLeg(driverData) {
  driverData = _.sortBy(driverData, 'rank');

  const teams = _.map(driverData, data => {
    return new Team(data.driverId.toString(),
                     new Player(data.driverId),
                     data.driverSkill);
  });

  let newSkills;
  let ratingsCalculated = false;
  while (!ratingsCalculated) {
    try {
      newSkills = TrueSkillCalculator
      .calculateNewRatings(GameInfo,
                           teams,
                           _.map(driverData, 'rank'));
      ratingsCalculated = true;
    } catch (err) {
      const lastRanking = _.last(driverData).rank;
      const indexToChange = _.findLastIndex(driverData,
                                            data => data.rank !== lastRanking);
      console.error(`SCORING ERROR: Changing the rating of: ` +
                    `${driverData[indexToChange].driverId}`);
      driverData[indexToChange].rank = lastRanking;
    }
  }

  return _.reduce(teams, (results, team) => {
    const player = team.getPlayers()[0];
    results[player.getId()] = newSkills[player];
    return results;
  }, {});
}

function scoreResults(raceResults, participants) {
  let driverSkills = {};

  _(raceResults)
  .mapValues(raceResult => {
    return _(raceResult.results)
    .flatMap(driverResults => {
      return _(driverResults.legs)
      .reject('unknownTime')
      .map((leg, index) => {
        return {
          driverId: driverResults.driverId,
          leg: index + 1,
          rank: leg.rank
        };
      })
      .value();
    })
    .groupBy('leg')
    .value();
  })

  .forEach((raceResult, raceId) => {
    _.forEach(raceResult, (legResults, legNum) => {
      console.log(`Scoring ${raceId} Leg ${Number(legNum)}`);
      const newSkills = scoreLeg(_.map(legResults, result => {
        const sk = _.has(_.last(driverSkills[result.driverId]), 'driverSkill') ?
                   _.last(driverSkills[result.driverId]).driverSkill :
                   GameInfo.getDefaultRating();
        return {
          driverId: result.driverId,
          driverSkill: sk,
          rank: result.rank
        };
      }));

      _.forOwn(newSkills, (newSkill, driverId) => {
        driverSkills[driverId] = driverSkills[driverId] || [];
        driverSkills[driverId].push({
          raceId: raceId,
          leg: legNum,
          driverSkill: newSkill
        });
      });
    });
  });

  _.forOwn(driverSkills, (skills, driverId) => {
    participants[driverId].driverSkills = skills;
    participants[driverId].driverSkill = _.last(skills).driverSkill;
  });

  return participants;
}

// output the ranked list of drivers
function printDriverStats(driverStats) {
  _(driverStats)
  .forEach((driver, index) => {
    console.log(`${index}\t${driver.driverSkill.conservativeRating}` +
                `\t${driver.starts}\t${driver.name}`);
  });
}

function main() {
  console.log('Getting data from Google Sheets');
  // spreadsheet key is the long id in the sheets URL
  const GoogleSpreadsheet = require('google-spreadsheet');
  const Sheet = new GoogleSpreadsheet(SHEET_KEY);

  Promise.all([getSheetRows(Sheet, 1).then(getParticipants),
               getSheetRows(Sheet, 3).then(getRaces),
               getSheetRows(Sheet, 2)
             ])
  .then(promiseResults => {
    console.log('Data from sheets pulled');
    let participants = promiseResults[0];
    const races = promiseResults[1];
    const legResults = getLegResults(promiseResults[2], races);

    const raceResults = getRaceResults(legResults, participants, races);
    const raceList = getRaceList(raceResults);

    participants = scoreResults(raceResults, participants);

    const driverStats = getDriverStats(raceResults, participants);

    console.log('Updating Firebase');
    const Firebase = require('firebase');
    const fbRef = new Firebase('https://dttdata.firebaseio.com/');
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
