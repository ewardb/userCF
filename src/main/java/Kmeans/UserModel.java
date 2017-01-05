package Kmeans;

import java.util.HashMap;

/**
 * Created by Qiang on 2016/12/24.
 */


public class UserModel {
    private int id;
    private HashMap<String,Integer> options = new HashMap<>();
    private int count;
    public UserModel(int id ){
        this.id = id;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public HashMap<String, Integer> getOptions() {
        return options;
    }

    public void setOptions(HashMap<String, Integer> options) {
        this.options = options;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    //
//    public HashMap<String,Integer> getOptions(String str){
//        if(str.equals("Action")){
//            return getAction();
//        }else if(str.equals("Crime")){
//            return getCrime();
//        }else if(str.equals("Fantasy")){
//            return getFantasy();
//        }else if(str.equals("Documentary")){
//            return getDocumentary();
//        }else if(str.equals("FilmNoir")){
//            return getFilmNoir();
//        }else if(str.equals("Musical")){
//            return getMusical();
//        }else if(str.equals("Mystery")){
//            return getMystery();
//        }else if(str.equals("Thriller")){
//            return getThriller();
//        }else if(str.equals("Romance")){
//            return getRomance();
//        }else if(str.equals("War")){
//            return getWar();
//        }else if(str.equals("Children")){
//            return getChildren();
//        }else if(str.equals("Adventure")){
//            return getAdventure();
//        }else if(str.equals("Animation")){
//            return getAnimation();
//        }else if(str.equals("Comedy")){
//            return getComedy();
//        }else if(str.equals("Drama")){
//            return getDrama();
//        }else if(str.equals("SciFi")){
//            return getSciFi();
//        }else{
//            return getWestern();
//        }
}