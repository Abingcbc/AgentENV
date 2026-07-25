<style>
  main { padding: 0 !important; }
</style>
<iframe
  id="swagger-frame"
  src="../swagger-ui.html"
  scrolling="no"
  style="width:100%; border:none; display:block; min-height:600px;">
</iframe>
<script>
document.getElementById('swagger-frame').addEventListener('load', function () {
  var frame = this;
  var resize = function () {
    frame.style.height = frame.contentDocument.documentElement.scrollHeight + 'px';
  };
  resize();
  new ResizeObserver(resize).observe(frame.contentDocument.body);
});
</script>
