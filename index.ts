// Import stylesheets
import "./style.css";

import { Observable, of, fromEvent, from } from "rxjs";
import {
  delay,
  switchMapTo,
  concatAll,
  count,
  scan,
  withLatestFrom,
  share,
  tap
} from "rxjs/operators";

const requestOne = of("first").pipe(delay(500));
const requestTwo = of("second").pipe(delay(800));
const requestThree = of("third").pipe(delay(1100));
const requestFour = of("fourth").pipe(delay(1400));
const requestFive = of("fifth").pipe(delay(1700));

const loadButton = document.getElementById("load");
const progressBar = document.getElementById("progress");
const content = document.getElementById("data");

// update progress bar as requests complete
const updateProgress = progressRatio => {
  console.log("Progress Ratio: ", progressRatio);
  progressBar.style.width = 100 * progressRatio + "%";
  if (progressRatio === 1) {
    progressBar.className += " finished";
  } else {
    progressBar.className = progressBar.className.replace(" finished", "");
  }
};
// simple helper to log updates
const updateContent = newContent => {
  content.innerHTML += newContent;
};

const displayData = data => {
  updateContent(`<div class="content-item">${data}</div>`);
};

// simulate 5 seperate requests that complete at variable length
const observables: Array<Observable<string>> = [
  requestOne,
  requestTwo,
  requestThree,
  requestFour,
  requestFive
];

const array$ = from(observables);

const requests$ = array$.pipe(concatAll());

const clicks$ = fromEvent(loadButton, "click");
// .pipe(tap(console.log));

const progress$ = clicks$.pipe(
  switchMapTo(requests$),
  share(),
  tap(console.log)
);
// .pipe();

const count$ = array$.pipe(
  count(),
  tap(a => console.log(a, count))
);

progress$
  .pipe(
    tap(displayData),
    scan(current => current + 1, 0),
    withLatestFrom(count$, (current, count) => current / count),
    tap(console.log)
  )
  .subscribe(updateProgress);
