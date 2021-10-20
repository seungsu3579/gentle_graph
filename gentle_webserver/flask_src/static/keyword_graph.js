function draw_window(){
  var width = window.outerWidth,
        height = window.outerHeight;

  var svg = d3.select("body").select("div.network").append("svg")
        .attr("width", width)
        .attr("height", height)
        .attr("class", "main");
  
  var force = d3.layout.force()
      .gravity(.03)
      .distance(100)
      .charge(-300)
      .size([width, height]);
  
  // var color = d3.scale.category10();


  d3.json("keyword_info_2.json", function(json) {
  
    var nodes = [{},];
    for (var word_id in json) {
      
      json[word_id].id = word_id;

      nodes.push(json[word_id]);
    }

    force
        .nodes(nodes)
        .start();

    var node = svg.selectAll(".node")
        .data(nodes)
      .enter().append("g")
        .attr("class", "node")
        .attr("id", function(d) { return d.id; })
        .call(force.drag);
  
    node.append("circle")
      .attr("r", 20)
      .style("display", "none");
      // .style("fill", function(d, i) { return color(i % 5); })
    
    force.on("tick", function() {
      node.attr("transform", function(d) { return "translate(" + d.x + "," + d.y + ")"; });
    });

    node.append("text")
        .attr("dx", 0)
        .attr("dy", ".35em")
        .text(function(d) { return d.name })
        .attr("class", "keyword")
        .attr("id", function(d) { return d.id; })
        .attr("font-size", function(d) { return d.weights * 40 + 10;});
  
    var root = nodes[0];
    root.radius = 0;
    root.fixed = true;
    svg.on("mousemove", function() {
      var p1 = d3.mouse(this);
      root.px = p1[0];
      root.py = p1[1];
      force.resume();
    });
  });
}


function draw_detail(node_id){
  var width = window.outerWidth,
        height = window.outerHeight;

  var svg = d3.select("body").select("div.network").append("svg")
        .attr("width", width)
        .attr("height", height)
        .attr("class", "detail");
        
  var force = d3.layout.force()
      .gravity(.2)
      .distance(1000)
      .charge(-1500)
      .size([width, height]);
      

  d3.json("keyword_info_2.json", function(json) {

          
    var target_node = json[node_id];
    var nodes = [{}, {'name' : target_node.name}];
    for (var idx in target_node.n_content) {
      var content = target_node.n_content[idx];
      content.type = 'content';
      // var tmp = {
      //   'id': content[0],
      //   'name': content[1],
      //   'url': content[2],
      //   'type': 'content',
      //   'weights' : idx,
      // };
      nodes.push(content);
    }

    for (var idx in target_node.n_key) {
      var content = target_node.n_key[idx];
      content.type = 'keyword';
      // var tmp = {
      //   'name' : target_node.n_key[idx],
      //   'type' : 'keyword',
      // };
      nodes.push(content);
    }

    var links = [];
    for (let idx = 0 ; idx < target_node.n_key.length ; idx++) {
      var tmp = {'source':1, 'target' : idx + 2};
      links.push(tmp);
    }

    force
        .nodes(nodes)
        .start();
  
    var node = svg.selectAll(".node")
        .data(nodes)
        .enter().append("g")
        .attr("class", "node")
        .attr("class", function (d) { return d.type;})
        .call(force.drag);
    
    node.append("circle")
        .attr("r", 50)
        .style("display", "none");
  
    
    var keyword = svg.selectAll(".keyword")
        .append("text")
        .attr("dx", 12)
        .attr("dy", ".35em")
        .text(function(d) { return d.name ; })
        .attr("font-size", function(d) { return d.weights * 50 + 30;});
  
        
    var contents = svg.selectAll(".content")
        .append("a")
        .attr("href", function (d) { return d.url ; })
        .append("text")
        .attr("dx", 12)
        .attr("dy", ".35em")
        .text(function(d) { return d.title ; })
        .attr("font-size", function(d) { return d.weights * 50 + 30;});
    
    var foci_keyword = {x: width * 0.25 , y: height * 0.5};
    var foci_content = {x: width * 0.25 , y: height * 0.5};

    function tick(){
      node.attr("transform", function(d) { return "translate(" + (d.type == 'keyword' ? d.x - width * 0.20 :  d.x + width * 0.10 )+ "," + (d.type == 'content' ? d.y + d.weights * 50 + 110 : d.y + 100 ) + ")"; });
    }

    force.on("tick", tick);

    var root = nodes[0];
    root.radius = 0;
    root.fixed = true;
    svg.on("mousemove", function() {
      var p1 = d3.mouse(this);
      root.px = p1[0];
      root.py = p1[1];
      force.resume();
    });

  });
}



function window_change() {
  location.reload();
}


// main

'use strict';
const promise = new Promise((resolve, reject) => {
  draw_window();
  setTimeout(()=> {
    resolve('');
  }, 1000);
});


promise.then(value => {
  const total_nodes = document.getElementsByClassName("node");
  
  for (let index = 0; index < total_nodes.length; index++) {

    total_nodes[index].addEventListener("click", function (e) {
      var word = e.target;
      const main_graph = document.querySelector("div.network svg.main");
      main_graph.style.display = 'none';

      
      draw_detail(word.id);

    
      // word
    });
  }
});


window.addEventListener("resize", window_change);


document.querySelector("div.network").addEventListener("contextmenu", function (e) {
  // const main_graph = document.querySelector("div.network svg");
  // main_graph.style.display = 'block';

  location.reload();

});
