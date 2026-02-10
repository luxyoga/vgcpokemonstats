// ============================================
// DOM Elements
// ============================================
const mobileMenuToggle = document.getElementById('mobileMenuToggle');
const nav = document.getElementById('nav');
const navLinks = document.querySelectorAll('.nav-link');
const contactForm = document.getElementById('contactForm');
const formMessage = document.getElementById('formMessage');
const galleryGrid = document.getElementById('galleryGrid');
const filterButtons = document.querySelectorAll('.filter-btn');
const lightbox = document.getElementById('lightbox');
const lightboxClose = document.getElementById('lightboxClose');
const lightboxImage = document.getElementById('lightboxImage');
const lightboxTitle = document.getElementById('lightboxTitle');
const lightboxDescription = document.getElementById('lightboxDescription');
const lightboxPrev = document.getElementById('lightboxPrev');
const lightboxNext = document.getElementById('lightboxNext');
const testimonialsTrack = document.getElementById('testimonialsTrack');
const testimonialsPrev = document.getElementById('testimonialsPrev');
const testimonialsNext = document.getElementById('testimonialsNext');
const testimonialsDots = document.getElementById('testimonialsDots');
const faqQuestions = document.querySelectorAll('.faq-question');

// ============================================
// Gallery Data
// ============================================
const galleryData = [
    {
        id: 1,
        image: 'https://via.placeholder.com/400x400/6366f1/ffffff?text=Warhammer+Standard',
        title: 'Space Marine Squad',
        description: 'Standard level painting for tabletop gaming',
        tags: ['standard', 'warhammer'],
        category: 'standard'
    },
    {
        id: 2,
        image: 'https://via.placeholder.com/400x400/ec4899/ffffff?text=D%26D+High',
        title: 'D&D Character',
        description: 'High quality character painting with detailed highlights',
        tags: ['high', 'dnd'],
        category: 'high'
    },
    {
        id: 3,
        image: 'https://via.placeholder.com/400x400/f59e0b/ffffff?text=Display+Quality',
        title: 'Competition Piece',
        description: 'Display quality with NMM and OSL techniques',
        tags: ['display', 'warhammer'],
        category: 'display'
    },
    {
        id: 4,
        image: 'https://via.placeholder.com/400x400/10b981/ffffff?text=Warhammer+High',
        title: 'Chaos Space Marine',
        description: 'High level painting with custom basing',
        tags: ['high', 'warhammer'],
        category: 'high'
    },
    {
        id: 5,
        image: 'https://via.placeholder.com/400x400/6366f1/ffffff?text=D%26D+Standard',
        title: 'D&D Party',
        description: 'Standard tabletop quality for gaming',
        tags: ['standard', 'dnd'],
        category: 'standard'
    },
    {
        id: 6,
        image: 'https://via.placeholder.com/400x400/ec4899/ffffff?text=Display+Masterpiece',
        title: 'Showcase Model',
        description: 'Competition-level display piece',
        tags: ['display', 'warhammer'],
        category: 'display'
    },
    {
        id: 7,
        image: 'https://via.placeholder.com/400x400/f59e0b/ffffff?text=Warhammer+Standard',
        title: 'Ork Army',
        description: 'Standard level army commission',
        tags: ['standard', 'warhammer'],
        category: 'standard'
    },
    {
        id: 8,
        image: 'https://via.placeholder.com/400x400/10b981/ffffff?text=D%26D+High',
        title: 'Dragon Miniature',
        description: 'High quality dragon with detailed scales',
        tags: ['high', 'dnd'],
        category: 'high'
    },
    {
        id: 9,
        image: 'https://via.placeholder.com/400x400/6366f1/ffffff?text=Display+Art',
        title: 'Artisan Model',
        description: 'Display quality with freehand details',
        tags: ['display', 'warhammer'],
        category: 'display'
    }
];

let currentGalleryFilter = 'all';
let currentLightboxIndex = 0;
let filteredGalleryData = [...galleryData];

// ============================================
// Testimonials Data
// ============================================
const testimonialsData = [
    {
        quote: "Absolutely incredible work! The attention to detail on my Space Marine army exceeded all expectations. Fast turnaround and excellent communication throughout.",
        author: "John D.",
        project: "Warhammer 40K Army - High Level",
        rating: 5
    },
    {
        quote: "Professional service from start to finish. The display quality piece I commissioned is now the centerpiece of my collection. Highly recommend!",
        author: "Sarah M.",
        project: "Competition Display Piece",
        rating: 5
    },
    {
        quote: "Great value for money with the standard level. My D&D party looks amazing on the tabletop. Will definitely commission more models in the future.",
        author: "Mike T.",
        project: "D&D Character Party - Standard",
        rating: 5
    },
    {
        quote: "The team was patient with all my questions and delivered exactly what I envisioned. The progress photos kept me updated every step of the way.",
        author: "Emma L.",
        project: "Custom Character Commission",
        rating: 5
    },
    {
        quote: "Fast shipping, careful packaging, and stunning results. My Chaos army is ready to dominate the tabletop!",
        author: "David R.",
        project: "Warhammer Army - Standard",
        rating: 5
    }
];

let currentTestimonialIndex = 0;

// ============================================
// Mobile Navigation Toggle
// ============================================
if (mobileMenuToggle) {
    mobileMenuToggle.addEventListener('click', () => {
        nav.classList.toggle('active');
        mobileMenuToggle.classList.toggle('active');
    });
}

// Close mobile menu when clicking a link
navLinks.forEach(link => {
    link.addEventListener('click', () => {
        if (window.innerWidth < 768) {
            nav.classList.remove('active');
            mobileMenuToggle.classList.remove('active');
        }
    });
});

// Close mobile menu when clicking outside
document.addEventListener('click', (e) => {
    if (window.innerWidth < 768) {
        if (!nav.contains(e.target) && !mobileMenuToggle.contains(e.target)) {
            nav.classList.remove('active');
            mobileMenuToggle.classList.remove('active');
        }
    }
});

// ============================================
// Smooth Scrolling for Navigation Links
// ============================================
navLinks.forEach(link => {
    link.addEventListener('click', (e) => {
        const href = link.getAttribute('href');
        if (href.startsWith('#')) {
            e.preventDefault();
            const targetId = href.substring(1);
            const targetElement = document.getElementById(targetId);
            if (targetElement) {
                const headerHeight = document.getElementById('header').offsetHeight;
                const targetPosition = targetElement.offsetTop - headerHeight;
                window.scrollTo({
                    top: targetPosition,
                    behavior: 'smooth'
                });
            }
        }
    });
});

// ============================================
// Gallery Filtering
// ============================================
function renderGallery() {
    galleryGrid.innerHTML = '';
    
    filteredGalleryData.forEach((item, index) => {
        const galleryItem = document.createElement('div');
        galleryItem.className = 'gallery-item';
        galleryItem.dataset.index = index;
        
        galleryItem.innerHTML = `
            <img src="${item.image}" alt="${item.title}" loading="lazy">
            <div class="gallery-item-overlay">
                <div class="gallery-item-title">${item.title}</div>
                <div class="gallery-item-tags">${item.tags.join(' • ')}</div>
            </div>
        `;
        
        galleryItem.addEventListener('click', () => openLightbox(index));
        galleryGrid.appendChild(galleryItem);
    });
}

function filterGallery(filter) {
    currentGalleryFilter = filter;
    
    // Update active filter button
    filterButtons.forEach(btn => {
        if (btn.dataset.filter === filter) {
            btn.classList.add('active');
        } else {
            btn.classList.remove('active');
        }
    });
    
    // Filter gallery data
    if (filter === 'all') {
        filteredGalleryData = [...galleryData];
    } else {
        filteredGalleryData = galleryData.filter(item => {
            return item.tags.includes(filter.toLowerCase()) || 
                   item.category === filter.toLowerCase();
        });
    }
    
    renderGallery();
}

// Initialize gallery filters
filterButtons.forEach(btn => {
    btn.addEventListener('click', () => {
        filterGallery(btn.dataset.filter);
    });
});

// Initialize gallery
renderGallery();

// ============================================
// Lightbox Functionality
// ============================================
function openLightbox(index) {
    currentLightboxIndex = index;
    const item = filteredGalleryData[index];
    
    lightboxImage.src = item.image;
    lightboxImage.alt = item.title;
    lightboxTitle.textContent = item.title;
    lightboxDescription.textContent = item.description;
    
    lightbox.classList.add('active');
    document.body.style.overflow = 'hidden';
    
    updateLightboxNavigation();
}

function closeLightbox() {
    lightbox.classList.remove('active');
    document.body.style.overflow = '';
}

function nextLightboxImage() {
    currentLightboxIndex = (currentLightboxIndex + 1) % filteredGalleryData.length;
    const item = filteredGalleryData[currentLightboxIndex];
    
    lightboxImage.src = item.image;
    lightboxImage.alt = item.title;
    lightboxTitle.textContent = item.title;
    lightboxDescription.textContent = item.description;
    
    updateLightboxNavigation();
}

function prevLightboxImage() {
    currentLightboxIndex = (currentLightboxIndex - 1 + filteredGalleryData.length) % filteredGalleryData.length;
    const item = filteredGalleryData[currentLightboxIndex];
    
    lightboxImage.src = item.image;
    lightboxImage.alt = item.title;
    lightboxTitle.textContent = item.title;
    lightboxDescription.textContent = item.description;
    
    updateLightboxNavigation();
}

function updateLightboxNavigation() {
    // Show/hide navigation buttons based on number of items
    if (filteredGalleryData.length <= 1) {
        lightboxPrev.style.display = 'none';
        lightboxNext.style.display = 'none';
    } else {
        lightboxPrev.style.display = 'flex';
        lightboxNext.style.display = 'flex';
    }
}

// Lightbox event listeners
if (lightboxClose) {
    lightboxClose.addEventListener('click', closeLightbox);
}

if (lightboxPrev) {
    lightboxPrev.addEventListener('click', prevLightboxImage);
}

if (lightboxNext) {
    lightboxNext.addEventListener('click', nextLightboxImage);
}

// Close lightbox on backdrop click
lightbox.addEventListener('click', (e) => {
    if (e.target === lightbox) {
        closeLightbox();
    }
});

// Keyboard navigation for lightbox
document.addEventListener('keydown', (e) => {
    if (lightbox.classList.contains('active')) {
        if (e.key === 'Escape') {
            closeLightbox();
        } else if (e.key === 'ArrowLeft') {
            prevLightboxImage();
        } else if (e.key === 'ArrowRight') {
            nextLightboxImage();
        }
    }
});

// ============================================
// Testimonials Carousel
// ============================================
function renderTestimonials() {
    testimonialsTrack.innerHTML = '';
    testimonialsDots.innerHTML = '';
    
    testimonialsData.forEach((testimonial, index) => {
        // Create testimonial item
        const testimonialItem = document.createElement('div');
        testimonialItem.className = 'testimonial-item';
        
        const stars = '★'.repeat(testimonial.rating);
        
        testimonialItem.innerHTML = `
            <div class="testimonial-stars">${stars}</div>
            <p class="testimonial-quote">"${testimonial.quote}"</p>
            <div class="testimonial-author">${testimonial.author}</div>
            <div class="testimonial-project">${testimonial.project}</div>
        `;
        
        testimonialsTrack.appendChild(testimonialItem);
        
        // Create dot
        const dot = document.createElement('button');
        dot.className = 'carousel-dot';
        if (index === 0) dot.classList.add('active');
        dot.setAttribute('aria-label', `Go to testimonial ${index + 1}`);
        dot.addEventListener('click', () => goToTestimonial(index));
        testimonialsDots.appendChild(dot);
    });
    
    updateTestimonialPosition();
}

function goToTestimonial(index) {
    currentTestimonialIndex = index;
    updateTestimonialPosition();
    updateTestimonialDots();
}

function nextTestimonial() {
    currentTestimonialIndex = (currentTestimonialIndex + 1) % testimonialsData.length;
    updateTestimonialPosition();
    updateTestimonialDots();
}

function prevTestimonial() {
    currentTestimonialIndex = (currentTestimonialIndex - 1 + testimonialsData.length) % testimonialsData.length;
    updateTestimonialPosition();
    updateTestimonialDots();
}

function updateTestimonialPosition() {
    const trackWidth = testimonialsTrack.offsetWidth;
    const translateX = -currentTestimonialIndex * trackWidth;
    testimonialsTrack.style.transform = `translateX(${translateX}px)`;
}

function updateTestimonialDots() {
    const dots = testimonialsDots.querySelectorAll('.carousel-dot');
    dots.forEach((dot, index) => {
        if (index === currentTestimonialIndex) {
            dot.classList.add('active');
        } else {
            dot.classList.remove('active');
        }
    });
}

// Initialize testimonials
renderTestimonials();

// Testimonial carousel event listeners
if (testimonialsNext) {
    testimonialsNext.addEventListener('click', nextTestimonial);
}

if (testimonialsPrev) {
    testimonialsPrev.addEventListener('click', prevTestimonial);
}

// Auto-advance testimonials (optional)
let testimonialInterval;
function startTestimonialAutoPlay() {
    testimonialInterval = setInterval(() => {
        nextTestimonial();
    }, 5000);
}

function stopTestimonialAutoPlay() {
    if (testimonialInterval) {
        clearInterval(testimonialInterval);
    }
}

// Pause auto-play on hover
if (testimonialsTrack) {
    testimonialsTrack.addEventListener('mouseenter', stopTestimonialAutoPlay);
    testimonialsTrack.addEventListener('mouseleave', startTestimonialAutoPlay);
}

// Start auto-play
startTestimonialAutoPlay();

// ============================================
// FAQ Accordion
// ============================================
faqQuestions.forEach(question => {
    question.addEventListener('click', () => {
        const isExpanded = question.getAttribute('aria-expanded') === 'true';
        const answer = question.nextElementSibling;
        
        // Close all other FAQs
        faqQuestions.forEach(q => {
            if (q !== question) {
                q.setAttribute('aria-expanded', 'false');
                q.nextElementSibling.classList.remove('active');
            }
        });
        
        // Toggle current FAQ
        if (isExpanded) {
            question.setAttribute('aria-expanded', 'false');
            answer.classList.remove('active');
        } else {
            question.setAttribute('aria-expanded', 'true');
            answer.classList.add('active');
        }
    });
});

// ============================================
// Contact Form Validation and Submission
// ============================================
function showFormMessage(message, type) {
    formMessage.textContent = message;
    formMessage.className = `form-message ${type}`;
    formMessage.style.display = 'block';
    
    // Scroll to message
    formMessage.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
    
    // Hide message after 5 seconds
    setTimeout(() => {
        formMessage.style.display = 'none';
    }, 5000);
}

function validateEmail(email) {
    const re = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    return re.test(email);
}

if (contactForm) {
    contactForm.addEventListener('submit', (e) => {
        e.preventDefault();
        
        const formData = new FormData(contactForm);
        const name = formData.get('name').trim();
        const email = formData.get('email').trim();
        const projectDescription = formData.get('projectDescription').trim();
        
        // Validation
        if (!name) {
            showFormMessage('Please enter your name.', 'error');
            return;
        }
        
        if (!email) {
            showFormMessage('Please enter your email address.', 'error');
            return;
        }
        
        if (!validateEmail(email)) {
            showFormMessage('Please enter a valid email address.', 'error');
            return;
        }
        
        if (!projectDescription) {
            showFormMessage('Please describe your project.', 'error');
            return;
        }
        
        // Simulate form submission
        // In production, this would send data to a backend
        showFormMessage('Thank you! Your message has been sent. We\'ll get back to you soon.', 'success');
        contactForm.reset();
        
        // In a real implementation, you would send the data to your backend:
        /*
        fetch('/api/contact', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({
                name,
                email,
                projectDescription,
                paintingLevel: formData.get('paintingLevel'),
                budget: formData.get('budget'),
                message: formData.get('message')
            })
        })
        .then(response => response.json())
        .then(data => {
            showFormMessage('Thank you! Your message has been sent.', 'success');
            contactForm.reset();
        })
        .catch(error => {
            showFormMessage('Sorry, there was an error sending your message. Please try again.', 'error');
        });
        */
    });
}

// ============================================
// Header Scroll Effect
// ============================================
let lastScroll = 0;
const header = document.getElementById('header');

window.addEventListener('scroll', () => {
    const currentScroll = window.pageYOffset;
    
    if (currentScroll > 100) {
        header.style.boxShadow = '0 4px 6px -1px rgba(0, 0, 0, 0.1)';
    } else {
        header.style.boxShadow = 'none';
    }
    
    lastScroll = currentScroll;
});

// ============================================
// Lazy Loading Images
// ============================================
if ('IntersectionObserver' in window) {
    const imageObserver = new IntersectionObserver((entries, observer) => {
        entries.forEach(entry => {
            if (entry.isIntersecting) {
                const img = entry.target;
                if (img.dataset.src) {
                    img.src = img.dataset.src;
                    img.removeAttribute('data-src');
                    observer.unobserve(img);
                }
            }
        });
    });
    
    document.querySelectorAll('img[data-src]').forEach(img => {
        imageObserver.observe(img);
    });
}

// ============================================
// Initialize on DOM Load
// ============================================
document.addEventListener('DOMContentLoaded', () => {
    // Update testimonial position on resize
    window.addEventListener('resize', () => {
        updateTestimonialPosition();
    });
    
    // Initial testimonial position update
    setTimeout(() => {
        updateTestimonialPosition();
    }, 100);
});
