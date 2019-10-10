var gulp = require('gulp');
var typescript = require('gulp-typescript');
var uglify = require('gulp-uglify-es').default;
var del = require('del');
var mocha = require('gulp-mocha');
var sourcemaps = require('gulp-sourcemaps');
var removeLogging = require('gulp-strip-debug');
var istanbul = require('gulp-istanbul');
var remapIstanbul = require('remap-istanbul/lib/gulpRemapIstanbul');
var tsProject = typescript.createProject('tsconfig.json');
var testProject = typescript.createProject('test/tsconfig.json');

gulp.task('compile', function () {
    return tsProject.src()
        .pipe(sourcemaps.init())
        .pipe(tsProject())
        .pipe(sourcemaps.write())
        .pipe(gulp.dest('dist'));
});

gulp.task('cleanup', function () {
    return del(['dist']);
});

gulp.task('logging', function () {
    return gulp.src(['dist/**/*.js'])
        .pipe(removeLogging())
        .pipe(gulp.dest('dist'));
});

gulp.task('minify', function () {
    return gulp.src(['dist/**/*.js'], { sourcemaps: true })
        .pipe(uglify())
        .pipe(gulp.dest('dist'));
});

gulp.task('clean:tests', function () {
    return del(['test/dist/**/*.js']);
});

gulp.task('compile:tests', function () {
    return testProject.src()
        .pipe(testProject())
        .pipe(gulp.dest('test/dist/'));
});

gulp.task('clean:cover', function () {
    return del(['coverage']);
});

gulp.task('cover:prepare', function () {
    return gulp.src(['dist/lib/**/*.js'])
        // Covering files
        .pipe(istanbul())
        // Force `require` to return covered files
        .pipe(istanbul.hookRequire());
});

gulp.task('run:tests', function () {
    return gulp.src('test/dist/**/*.js')
        .pipe(mocha({
            reporter: 'spec',
            exit: true
        }));
});

gulp.task('cover:test', function () {
    return gulp.src('test/**/*.js')
        .pipe(mocha({
            reporter: 'spec',
            exit: true
        }))
        .pipe(istanbul.writeReports({
            reporters: ['json']
        }));
});

gulp.task('cover:remap', function () {
    return gulp.src('./coverage/coverage-final.json')
        .pipe(remapIstanbul({
            reports: {
                'html': './coverage'
            }
        }));
});

gulp.task('build', gulp.series('cleanup', 'compile', 'logging', 'minify'));
gulp.task('build:debug', gulp.series('cleanup', 'compile'));
gulp.task('test', gulp.series('cleanup', 'compile', 'clean:tests', 'compile:tests', 'run:tests'));
gulp.task('cover', gulp.series('cleanup', 'compile', 'clean:cover', 'cover:prepare', 'cover:test', 'cover:remap'));