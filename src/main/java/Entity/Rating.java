package Entity;

/**
 * Created by wq on 16-11-24.
 */
public class Rating {
    private int movieId;
    private int userId;
    private int rating;

    public Rating() {
    }

    public Rating(int movieId, int userId, int rating) {
        this.movieId = movieId;
        this.userId = userId;
        this.rating = rating;
    }

    public int getMovieId() {

        return movieId;
    }

    public void setMovieId(int movieId) {
        this.movieId = movieId;
    }

    public int getUserId() {
        return userId;
    }

    public void setUserId(int userId) {
        this.userId = userId;
    }

    public int getRating() {
        return rating;
    }

    public void setRating(int rating) {
        this.rating = rating;
    }

}
