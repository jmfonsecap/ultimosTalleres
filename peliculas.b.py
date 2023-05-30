from mrjob.job import MRJob
from mrjob.step import MRStep
class MovieRatingStatistics(MRJob):


    def mapper(self, _, line):
        # Divide cada línea en campos separados por coma
        for w in line.split():
            filing = w.split(',')
            user= filing[0]
            movie= filing[1] 
            rating= filing[2]
            genre= filing[3]
            date= filing[4]
            yield user, float(rating)

    def reducer(self, key, values):
        total_rating= 0
        count = 0

        for rating in values:
            total_rating += rating
            count += 1

        average_rating = total_rating / count

        yield key, (average_rating,count)
class DayMoreviews(MRJob):


    def mapper(self, _, line):
        # Divide cada línea en campos separados por coma
        for w in line.split():
            filing = w.split(',')
            user= filing[0]
            movie= filing[1] 
            rating= filing[2]
            genre= filing[3]
            date= filing[4]
            yield date, 1

    def reducer(self, key, values):
        views = sum(values)
        yield None, (views, key)

    def find_max_views(self, _, views_and_dates):
        max_views = 0
        max_date = None
        for view_date in views_and_dates:
            views, date = view_date
            if views > max_views:
                max_views = views
                max_date = date
        yield max_date, max_views

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer),
            MRStep(reducer=self.find_max_views)
        ]
class DayLessviews(MRJob):

    def mapper(self, _, line):
        # Divide cada línea en campos separados por coma
        for w in line.split():
            filing = w.split(',')
            user = filing[0]
            movie = filing[1]
            rating = filing[2]
            genre = filing[3]
            date = filing[4]
            yield date, 1

    def reducer(self, key, values):
        views = sum(values)
        yield None, (views, key)

    def find_min_views(self, _, views_and_dates):
        min_views = float('inf')
        min_date = None
        for views, date in views_and_dates:
            if views < min_views:
                min_views = views
                min_date = date
        yield min_date, min_views

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer),
            MRStep(reducer=self.find_max_views)
        ]
class UsersPerMovieAndReviews(MRJob):
    def mapper(self, _, line):
        # Divide cada línea en campos separados por coma
        for w in line.split():
            filing = w.split(',')
            user= filing[0]
            movie= filing[1] 
            rating= filing[2]
            genre= filing[3]
            date= filing[4]
            yield movie, float(rating)

    def reducer(self, key, values):
        total_rating= 0
        count = 0

        for rating in values:
            total_rating += rating
            count += 1

        average_rating = total_rating / count

        yield key, (average_rating,count)
class DayLowestRating(MRJob):
    def mapper(self, _, line):
        for w in line.split():
            filing = w.split(',')
            user = filing[0]
            movie = filing[1] 
            rating = filing[2]
            genre = filing[3]
            date = filing[4]
            yield date, float(rating)

    def reducer(self, key, values):
        total_rating = 0
        count = 0

        for rating in values:
            total_rating += rating
            count += 1

        average_rating = total_rating / count

        yield None, (average_rating, key)  # Yield average rating and date

    def find_lowest_average_rating(self, _, values):
        min_average_rating = float('inf')
        min_date = None

        for average_rating, date in values:
            if average_rating < min_average_rating:
                min_average_rating = average_rating
                min_date = date

        yield min_date, min_average_rating

    def steps(self):
        return [
            MRStep(mapper=self.mapper, reducer=self.reducer),
            MRStep(reducer=self.find_lowest_average_rating)
        ]
class DayHighestRating(MRJob):
    def mapper(self, _, line):
        for w in line.split():
            filing = w.split(',')
            user = filing[0]
            movie = filing[1] 
            rating = filing[2]
            genre = filing[3]
            date = filing[4]
            yield date, float(rating)

    def reducer(self, key, values):
        total_rating = 0
        count = 0

        for rating in values:
            total_rating += rating
            count += 1

        average_rating = total_rating / count

        yield None, (average_rating, key)  # Yield average rating and date

    def find_highest_average_rating(self, _, values):
        max_average_rating = 0
        max_date = None

        for average_rating, date in values:
            if average_rating > max_average_rating:
                max_average_rating = average_rating
                max_date = date

        yield max_date, max_average_rating

    def steps(self):
        return [
            MRStep(mapper=self.mapper, reducer=self.reducer),
            MRStep(reducer=self.find_lowest_average_rating)
        ]
class LowestAndHighestByGenre(MRJob):
    def mapper(self, _, line):
        for w in line.split():
            filing = w.split(',')
            user = filing[0]
            movie = filing[1] 
            rating = filing[2]
            genre = filing[3]
            date = filing[4]
            yield genre, (float(rating), movie)

    def reducer(self, key, values):
        ratings = list(values)
        min_rating = min(ratings)
        max_rating = max(ratings)
        yield key, (min_rating[1],max_rating[1])
if __name__ == '__main__':
    MovieRatingStatistics.run()
    DayMoreviews.run()
    DayMoreviews.run()
    UsersPerMovieAndReviews.run()
    DayLowestRating.run()
    DayHighestRating.run()
    LowestAndHighestByGenre.run()
