// Reserved for a future shared Frame type.
//
// `Frame` is currently defined in `rvo-buffer` because it holds an
// `opencv::core::Mat`, and `rvo-buffer` already carries the OpenCV dependency.
// Moving `Frame` here would require every crate that imports from `rvo-core`
// to also pull in OpenCV, which defeats the purpose of a thin shared-types
// crate.
//
// The right fix is to decouple the frame *handle* (an id + timestamp +
// optional Arc<Mat>) from the frame *store* (the circular buffer). When that
// decoupling happens, the handle type will live here and `rvo-buffer` will
// depend on `rvo-core` rather than the other way around.
//
// Until then: do not add OpenCV imports here.
