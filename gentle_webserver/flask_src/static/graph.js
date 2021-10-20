function draw_window(){
  var width = window.outerWidth,
        height = window.outerHeight;

  var svg = d3.select("body").select("div.network").append("svg")
        .attr("width", width)
        .attr("height", height);
  
  var force = d3.layout.force()
      .gravity(.05)
      .distance(100)
      .charge(-200)
      .size([width, height]);
  
  d3.json("graphFile.json", function(json) {
  
    force
        .nodes(json.nodes)
        .links(json.links)
        .start();
  
    var link = svg.selectAll(".link")
        .data(json.links)
      .enter().append("line")
        .attr("class", "link")
      .style("stroke-width", function(d) { return Math.sqrt(d.weight); });
  
    var node = svg.selectAll(".node")
        .data(json.nodes)
      .enter().append("g")
        .attr("class", "node")
        .call(force.drag);
  
    node.append("circle")
        .attr("r","10");
  
    node.append("text")
        .attr("dx", 12)
        .attr("dy", ".35em")
        .text(function(d) { return d.name });
  
    force.on("tick", function() {
      link.attr("x1", function(d) { return d.source.x; })
          .attr("y1", function(d) { return d.source.y; })
          .attr("x2", function(d) { return d.target.x; })
          .attr("y2", function(d) { return d.target.y; });
  
      node.attr("transform", function(d) { return "translate(" + d.x + "," + d.y + ")"; });
    });
  });
}


function window_change() {
  location.reload();
}

window.addEventListener("resize", window_change)

draw_window();