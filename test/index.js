'use strict';

var RateLimiter = require('../index');

var numLimiters = 2;
var limiters = [];

for (var i = 0; i < numLimiters; i++) {
    var limiter = new RateLimiter({
        port: 3050,
        seeds: [{
            address: 'localhost',
            port: 3050
        }],
        interval: 10000,
    })
    limiter.setup();
    limiters.push(limiter);
}


setInterval(function() {
    var iterations = 100; //Math.random() * 4;
    var rejected = 0;
    for (var i = 0; i < iterations; i++) {
        if (!limiters[i % numLimiters].check('foo' + i % 20, 5)) {
            rejected++;
        }
    }
    console.log('rejected ' + rejected + '/' + iterations);
}, 1000);
