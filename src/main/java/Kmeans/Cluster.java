package Kmeans;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by Qiang on 2016/12/24.
 */
public class Cluster {
    private int clusterid;
    private UserModelXY preCenterid;
    private UserModelXY newCenterid;
    private List<UserModelXY> listPoint;

    public Cluster(UserModelXY p){
        this.preCenterid = p;
    }
    public void setClusterid(int clusterid){
        this.clusterid = clusterid;
    }
    public int getClusterid(){
        return this.clusterid;
    }
    public void setPreCenterid( UserModelXY p){
        this.preCenterid = p;
    }
    public UserModelXY getPreCenterid(){
        return this.preCenterid;
    }

    public void serNewCenterid( UserModelXY p){
        this.newCenterid = p;
    }
    public UserModelXY getNewCenterid(){
        return this.newCenterid;
    }


    public void addPoint(UserModelXY p){
        if(this.listPoint == null){
            List<UserModelXY> listPoint = new LinkedList<UserModelXY>();
            listPoint.add(p);
            this.listPoint = listPoint;
        } else{
            this.listPoint.add(p);
        }
    }

    public List<UserModelXY> getListPoint(){
        return this.listPoint;
    }

    public void calCenterid(){
        double action = 0.0;
        double adventure= 0.0;
        double animation= 0.0;
        double children= 0.0;
        double comedy= 0.0;
        double crime= 0.0;
        double documentary= 0.0;
        double drama= 0.0;
        double fantasy= 0.0;
        double filmnoir= 0.0;
        double horror= 0.0;
        double musical= 0.0;
        double mystery= 0.0;
        double romance= 0.0;
        double scifi= 0.0;
        double thriller= 0.0;
        double war= 0.0;
        double western= 0.0;
        if(listPoint == null){
            return;
        }
        for(UserModelXY p: listPoint){
            action += p.getAction();
            adventure += p.getAdventure();
            animation += p.getAnimation();
            children += p.getChildren();
            comedy += p.getComedy();
            crime += p.getCrime();
            documentary += p.getDocumentary();
            drama += p.getDrama();
            fantasy += p.getFantasy();
            filmnoir += p.getFilmnoir();
            horror += p.getHorror();
            musical += p.getMusical();
            mystery +=p.getMystery();
            romance += p.getRomance();
            scifi +=p.getScifi();
            thriller +=p.getThriller();
            war +=p.getWar();
            western += p.getWestern();
        }
        action /=listPoint.size();
        adventure/=listPoint.size();
        animation/=listPoint.size();
        children/=listPoint.size();
        comedy/=listPoint.size();
        crime/=listPoint.size();
        documentary/=listPoint.size();
        drama/=listPoint.size();
        fantasy/=listPoint.size();
        filmnoir/=listPoint.size();
        horror/=listPoint.size();
        musical/=listPoint.size();
        mystery/=listPoint.size();
        romance/=listPoint.size();
        scifi/=listPoint.size();
        thriller/=listPoint.size();
        war/=listPoint.size();
        western/=listPoint.size();
        UserModelXY p = new UserModelXY(action,adventure,animation,children,comedy,crime,documentary,drama,fantasy,filmnoir,horror,musical,mystery,romance,scifi, thriller, war,western);  //新的质心是该簇所包含的point点的x坐标平均 和 y坐标 平均 ； 并设置新的质心
        this.serNewCenterid(p);
    }


}
