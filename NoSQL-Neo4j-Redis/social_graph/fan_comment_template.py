from py2neo import Graph, NodeMatcher, Node, Relationship
import json
import sys
sys.path
sys.path.append("../")
from utils import utils as ut
import uuid


class FanGraph(object):
    """
    This object provides a set of helper methods for creating and retrieving Nodes and relationship from
    a Neo4j database.
    """

    # Connects to the DB and sets a Graph instance variable.
    # Also creates a NodeMatcher, which is a py2neo class.
    def __init__(self,  auth, host, port, secure=False, ):
        self._graph = Graph(secure=secure,
                            bolt=True,
                            auth=auth,
                            host=host,
                            port=port)
        self._node_matcher = NodeMatcher(self._graph)

    def run_match(self, labels=None, properties=None):
        """
        Uses a NodeMatcher to find a node matching a "template."
        :param labels: A list of labels that the node must have.
        :param properties: A parameter list of the form prop1=value1, prop2=value2, ...
        :return: An array of Node objects matching the pattern.
        """
        #ut.debug_message("Labels = ", labels)
        #ut.debug_message("Properties = ", json.dumps(properties))

        if labels is not None and properties is not None:
            result = self._node_matcher.match(labels, **properties)
        elif labels is not None and properties is None:
            result = self._node_matcher.match(labels)
        elif labels is None and properties is not None:
            result = self._node_matcher.match(**properties)
        else:
            raise ValueError("Invalid request. Labels and properties cannot both be None.")

        # Convert NodeMatch data into a simple list of Nodes.
        full_result = []
        for r in result:
            full_result.append(r)

        return full_result

    def find_nodes_by_template(self, tmp):
        """

        :param tmp: A template defining the label and properties for Nodes to return. An
         example is { "label": "Fan", "template" { "last_name": "Ferguson", "first_name": "Donald" }}
        :return: A list of Nodes matching the template.
        """
        labels = tmp.get('label')
        props = tmp.get("template")
        result = self.run_match(labels=labels, properties=props)
        return result

    # Create and save a new node for  a 'Fan.'
    def create_fan(self, uni, last_name, first_name):
        if self.get_fan(uni) is None:
            n = Node("Fan", uni=uni, last_name=last_name, first_name=first_name)
            tx = self._graph.begin(autocommit=True)
            return tx.create(n)
        else: 
            print("fan with this uni already exists")
    # Given a UNI, return the node for the Fan.
    def get_fan(self, uni):
        n = self.find_nodes_by_template({"label": "Fan", "template": {"uni": uni}})
        if n is not None and len(n) > 0:
            n = n[0]
        else:
            n = None
        return n

    def get_player(self, player_id):
        n = self.find_nodes_by_template({"label": "Player", "template": {"player_id": player_id}})
        if n is not None and len(n) > 0:
            n = n[0]
        else:
            n = None

        return n
    def create_player(self, player_id, last_name, first_name):
        if self.get_player(player_id) is None:
            n = Node("Player", player_id=player_id, last_name=last_name, first_name=first_name)
            tx = self._graph.begin(autocommit=True)
            tx.create(n)
            return n
        else: 
            print("player with this uni already exists")



    def create_team(self, team_id, team_name):
        if self.get_team(team_id) is None:
            n = Node("Team", team_id=team_id, team_name=team_name)
            tx = self._graph.begin(autocommit=True)
            tx.create(n)
            return n
        else: 
            print("team with this uni already exists")

    def get_team(self, team_id):
        n = self.find_nodes_by_template({"label": "Team", "template": {"team_id": team_id}})
        if n is not None and len(n) > 0:
            n = n[0]
        else:
            n = None

        return n

    def create_supports(self, uni, team_id):
        """
        Create a SUPPORTS relationship from a Fan to a Team.
        :param uni: The UNI for a fan.
        :param team_id: An ID for a team.
        :return: The created SUPPORTS relationship from the Fan to the Team
        """
        f = self.get_fan(uni)
        t = self.get_team(team_id)
        if f is None:
            print ("fan uni is wrong ")
            return None
        elif t is None:
            print("team id is wrong")
            return None
        r = Relationship(f, "SUPPORTS", t)
        tx = self._graph.begin(autocommit=True)
        tx.create(r)
        return r

    # Create an APPEARED relationship from a player to a Team
    def create_appearance(self, player_id, team_id):
        try:
            f = self.get_player(player_id)
            t = self.get_team(team_id)
            r = Relationship(f, "APPEARED", t)
            tx = self._graph.begin(autocommit=True)
            tx.create(r)
        except Exception as e:
            print("create_appearances: exception = ", e)

    # Create a FOLLOWS relationship from a Fan to another Fan.
    def create_follows(self, follower, followed):
        f = self.get_fan(follower)
        t = self.get_fan(followed)
        if f is None:
            print ("follower uni is wrong ")
            return None
        elif t is None:
            print("followed id is wrong")
            return None
        r = Relationship(f, "FOLLOWS", t)
        tx = self._graph.begin(autocommit=True)
        tx.create(r)

    def get_comment(self, comment_id):
        n = self.find_nodes_by_template({"label": "Comment", "template": {"comment_id": comment_id}})
        if n is not None and len(n) > 0:
            n = n[0]
        else:
            n = None

        return n

    def create_comment(self, uni, comment, team_id=None, player_id=None):
        """
        Creates a comment
        :param uni: The UNI for the Fan making the comment.
        :param comment: A simple string.
        :param team_id: A valid team ID or None. team_id and player_id cannot BOTH be None.
        :param player_id: A valid player ID or None
        :return: The Node representing the comment.
        """
        if uni is None or comment is None or (team_id is None and player_id is None):
            raise ValueError("Bad input")
        tx=self._graph.begin()
        try:
            comment_id=str(uuid.uuid4())
            new_c=Node("Comment",comment=comment, comment_id=comment_id)
            tx.create(new_c)
            f= self.get_fan(uni)
            p=None
            t=None
            if f is None:
                raise ValueError("Fan not found")
            if player_id is not None:
                p=self.get_player(player_id)
                if p is None:
                    raise ValueError("Player nto found")
            if team_id is not None:
                t=self.get_team(team_id)
                if t is None:
                    raise ValueError("Team not found")
            r1=Relationship(f, "COMMENT_BY", new_c)
            tx.create(r1)

            if p is not None:
                r2= Relationship(new_c, "COMMENT_ON", p)
                tx.create(r2)
            if t is not None:
                r3=Relationship(new_c, "COMMENT_ON", t)
                tx.create(r3)
            tx.commit()
            return comment_id
        except Exception as e:
            print("Exception: ", e)
            tx.rollback()
            raise e 

    def create_sub_comment(self, uni, origin_comment_id, comment):
        """
        Create a sub-comment (response to a comment or response) and links with parent in thread.
        :param uni: ID of the Fan making the comment.
        :param origin_comment_id: Id of the comment to which this is a response.
        :param comment: Comment string
        :return: Created comment.
        """
        if uni is None or origin_comment_id is None or comment is None:
            raise ValueError("Bad input")
        tx=self._graph.begin()
        try:
            comment_id=str(uuid.uuid4())
            new_c=Node("Comment",comment=comment, comment_id=comment_id)
            tx.create(new_c)
            f= self.get_fan(uni)
            old_c= self.get_comment(origin_comment_id)
            if f is None or old_c is None:
                raise ValueError("Fan or origin_comment not found")
            r1= Relationship(f, "RESPONSE_BY", new_c)
            tx.create(r1)
            r2=Relationship(new_c,"RESPONSE_TO", old_c)
            tx.create(r2)
            tx.commit()
            return new_c
        except Exception as e:
            print("Exception: ",e)
            tx.rollback()
            raise e 
        # q= "match (c:Comment {comment_id: {cid}}) <= [response:RESPONSE_TO] - (sc:Comment) return sc.response.c"
        # c= self._graph.run(q, cid-comment_id)
        # return c 


    def get_player_comments(self, player_id):
        """
        Gets all of the comments associated with a player, all of the comments on the comment and comments
        on the comments, etc. Also returns the Nodes for people making the comments.
        :param player_id: ID of the player.
        :return: Graph containing comment, comment streams and commenters.
        """
        q= 'match (fan)-[by:COMMENT_BY]->(comment)-[on:COMMENT_ON]->(player:PLAYER {player_id:{pid}}) '+\
            'return player, on, comment, by, fan'
        result = self._graph.run(q, pid=player_id)
        return result

    def get_team_comments(self, team_id):
        """
        Gets all of the comments associated with a teams, all of the comments on the comment and comments
        on the comments, etc. Also returns the Nodes for people making the comments.
        :param player_id: ID of the team.
        :return: Graph containing comment, comment streams and commenters.
        """
        q="match (fan)-[by:COMMENT_BY]->(comment)-[on:COMMENT_ON]->(team:Team (team_id: (team_id))) return player, on, comment, by, fan"
        result = self._graph.run(q, pid=team_id)
        return result













"""
bryankr01   CHN
scherma01   WAS
abreujo02   CHA
ortizda01   BOS
jeterde01   NYA
"""
